阿里巴巴中间件大赛第二赛季代码
================

#### 比赛链接
[比赛主页](https://tianchi.shuju.aliyun.com/programming/information.htm?spm=5176.100067.5678.2.vWrFBL&raceId=231533)
[接口说明地址](https://code.aliyun.com/MiddlewareRace/order-system?spm=5176.100068.555.7.25yKHP)
[Demo地址](https://code.aliyun.com/MiddlewareRace/order-system-impl/tree/master)

#### 成绩
比赛的最终排名**13**（**3275s**构建时间 **9.8w**查询）
现在仓库中的是修改之后的代码 **1985s**构建时间 **31.5w**查询

#### 比赛环境、数据

正式环境中的数据分为**order**,**buyer**和**good**三种，实际数据量分别在4亿、800w和400w。
比赛的机器配置为**3 * 1.5T**的硬盘 **8**核CPU和 **4G**的JVM内存

#### 查询和索引结构

由于原始的order文件较大，因此采用hash的思路将大文件文件切割成若干个小文件。

1. 查询1:索引的思路就是根据orderid来hash切割order文件，文件里面记录了每个orderid对应的**order原始文件**、**偏移offset**和**记录长度length**。
查找的时候根据orderid去hash索引文件中查找对应的索引，并去原始的order文件中查找记录。
2. 查询2、3、4:多条记录查询，此时需要遍历整个索引文件并查询多条原始记录，性能较低。所以此处在hash结束之后还需要增加一个步骤——**对同一个buyerid(或者goodid)的order索引记录进行group**,生成group之后的**聚集一级索引**(索引的结构还是和查询1的一样),对2、3查询生成一个大的group索引文件(查询4的索引文件结构和3一样)，在这个文件中，同一个buyerid(或者是goodid)的**order索引信息**放在一起，这样可以避免遍历整个索引文件。
定位某个buyer(或者good)对应的**order索引信息**的结构是**偏移offset**和**索引记录数count**。
查询的时候根据buyerid(或者是goodid)找到对应order索引信息的**offset**和**count**,之后去聚集一级索引文件中读出这个buyerid(或者goodid)对应的order的索引信息(结构如查询1的hash索引文件)，最后去order的原始文件中查找原始order的记录。由于buyer和good的记录分别只有800w和400w，可以考虑将**offset和count二级索引结构放在内存map中**，key-value格式为**buyerid(或者goodid)**:**{offset,count}**(后面有内存开销的估算)。
3. Join操作:4种查询可能都会涉及到buyer和good原始数据字段的查询，此时需要根据对应的buyerid或者(goodid)去对应的buyer(或者good)文件中读取对应的字段。这部分信息索引的结构也是**buyer(good)原始文件**、**偏移offset**和**记录长度length**。同样的，这部分也和按照buyerid(或者goodid)放到内存中，和之前的group信息一起。
4. 最后结合order查询和join, buyer和good的的内存map索引结构是:**buyerid(或者gooid)**:**{fileIndex,originalOffset,origalLength,offset,count}**，其中value格式的前3个字段用于join操作，后面2个字段用于查找order的聚集一级索引中改buyerid(或者goodid)对应的order原始文件的索引信息。这个内存value对应的类为`MetaTuple`。Order的一级聚集索引的结构和map索引结构value的前3个字段相同，用于查询order的原始数据，其对应的类为`IndexFileTuple`。
5. 内存估算:放在内存中map的格式如上，其中key为buyerid或者goodid，其大小在20bytes左右，后面的索引信息按字节存储，类型分别为int,long,int,long,int。一条记录的大小为28bytes,结合其他map的控制信息，估计一条记录占用80bytes的内存，1200w条占用的空间大概在915M左右，完全在4G的JVM内存允许的范围内。实际运行的结果是查询是内存使用恒定在1.8G，也说明了将order的二级索引和buyer、good的索引放在内存中完全没问题。

#### 索引建立和查询的优化

* 索引建立优化

1. 3个线程同时建立3个order索引(查询3,4的索引一致)，并将索引分别写到3个磁盘上，这样避免多线程访问一个磁盘可能带来的磁头寻道的开销。
2. order索引建立完毕之后再初始化内存map并进行一级索引group以及buyer、good索引的建立，防止一开始就占用了内存导致的IO性能降低。
3. 建立索引时避免使用`String.split()`进行切割，这样会导致大量的无用切割和复制，具体优化的方法可以参考`StringUtils.createKVMapFromLineWithSet()`。
4. 由于buyer和good的数目已已知，因此内存map初始化的时候可以直接设置大小，并设置`loadFactor`为1f，如`goodMemoryIndexMap = new HashMap<>(4194304, 1f)`，这样可以防止map不断扩容带来的rehash开销。

* 查询优化 

1. 查询2、3、4和join都需要读多条原始文件的数据，而这些数据可能有的是在同一个文件中的，而有的记录可能靠得比较近，如果每条记录都单独打开文件并读取的话会带来不必要的开销，并且也可能无法利用系统的IO缓冲区。此处的优化是读取原始文件时，**将同一个原始文件的索引信息聚在一起，并按照记录的offset排序**，这样能最大程度的减少不必要的IO开销并重复利用系统IO缓冲区。具体可以参考`JoinGroupHelper.createDataAccessSequence()`
2. 比赛提供的是8核CPU，所以查询时可以利用多核的优势。由于在读取order原始数据的时候可能涉及到多个文件，因此将一个文件的读取作为一个`Callable`传递给**线程池**，此处采用的是fixed的线程池，查询2、3、4各有一个`corePoolSize`为8的线程池。具体的`Callable`实现类为`OrderDataSearchCallable`。虽然Join操作也可以使用线程池，但是线上测试的结果显示join时间占总时间较小，所以没有采用。
3. Join优化1(这里以查询2为例，查询3、4同理):查询一个buyer在一个时间段内的所有订单并join时，需要join的buyer信息是一定的，因此对应的buyer信息只需要查询一次即可。
4. Join优化2:由于**某些字段只会在某些文件中出现**(如price字段只会在good原始文件中出现)，因此对于查询3、4，如果查询的key只有在good或者order中出现(good只需要join一次，而order不需要join)，那么就不需要join buyer信息。具体的字段信息判断在`JoinGroupHelper.getKeyJoin()`中。
5. 查询4的专用优化:查询4是根据goodid去查找所有order信息然后join，最后对某一个字段求和。如果这个字段信息只有在good文件中才会出现，那么只需要根据内存map的信息去good原始文件中获得这个字段的值（如果是long或者double类型的话），然后从内存map中获得这个goodid对应的order的订单数量(直接取map中的**count**值)，直接相乘返回即可(因为join的good信息是一样的，所以可以直接相乘)。这部分可以参考查询4的代码:`OrderSystemImpl.sumOrdersByGood()`
