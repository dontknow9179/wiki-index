1. 概述

本次PJ我做的是建立Wikipedia索引。先用hadoop集群计算出TF值，DF值，TF 文件的倒排索引和单词在文章中的POSITION，文章ID对应的TITLE，然后将索引存入MySQL数据库，利用java实现了简单的检索功能。

2. Map Reduce

使用hadoop集群得到了以下几种文件：

- id_title: key=id; value=title
- term_tf_id: key=term, tf; value=id
- term_df_offset_length: key=term; value=df, offset, length
- term_id_position: key=term, id; value=position 

2.1 标题索引(id_title)

标题索引记录的是每个页面(page)的id和title。由于 hadoop 默认 Mapper 输入为按行输入，对于 XML格式的文件，我们需要将 XML切分为许多小的page作为 Mapper的输入，所以需要自定义hadoop的InputFormat类。为此我使用了网上的一份代码切分XML作为Mapper的输入。http://blog.csdn.net/doegoo/article/details/50401080 为了方便处理XML中的页面内容，还使用了WikiClean这个第三方库把Wiki Markup转化纯文本，并使用它获取文章的id和title。 https://github.com/lintool/wikiclean 

map输入：key为该页面在xml中的起始位置offset，value为该页面的xml内容

map输出：key为文章id，value为文章标题

不需要reduce过程，文件大小为115MB

2.2 TF文件(term_tf_id)

一开始想把所有的信息都放在一起，利用term索引，但是又希望索引结果能按tf排序，所以必须先把tf作为key的一部分。于是有了这个索引key=term，tf； value=id。一开始把id和title合并为一个Text作为value，结果输出的结果有23G，因为一篇文章的标题会出现其所含有词项的数目次，冗余过多了，但是希望能在返回id时也返回title，于是才建了前面所说的key=id,value=title的索引。此外，对于某一个单词，包含它的页面中的优先级，在TF之外还考虑了标题的影响，即如果某一个单词出现在页面的标题当中，则适当提高它的权重。因此输出的tf是处理加工过的tf_。

    double tf = 1.0 * entry.getValue() / length;
    int inTitle = (title.indexOf(key_) == -1) ? 0 : 1;
    double tf_ = (0.85 * tf + 0.15 * inTitle) * 1000.0;

map输入：key为该页面在xml中的起始位置offset，value为该页面的xml内容

map输出：key为(term,tf), value=id

为了提取英文单词，定义了一个split函数，利用正则表达式将以字符数组形式表示的文本转换为存放了page中的英文单词的字符串数组。

为了让展示给用户的搜索结果按tf_从高到低排序，写了一个StringDoubleWritable类存放key，实现了WritableComparable接口的compareTo方法。

不需要reduce过程，文件大小为6.45GB。

2.3 基于TF文件的倒排索引(term_df_offset_length)

尽管TF文件有序，但依然无法将这个大文件放入普通的MySQL数据库中进行检索。查阅资料得知可以对TF文件建立进一步的索引，例如key=term; value=(offset,length)，就可以利用一个词，在文件中寻找需要读取的内容。

其实最初希望通过对文件再进行一次mapreduce找出每个词的df并且将格式整理为key=term，value=(df, ((id0,tf0)(id1,tf1)...))这样的效果，在初始的map结束后处理为了key=term, value=(id,tf)的形式，也在reduce中算出了df，但是不知道怎么形成一个内部按照tf值排序的value，并且发现这样做依然面临着不知道如何从一个大文件中寻找一个key对应的value的问题。因此感受到建立TF文件的倒排索引的必要性，而在计算TF文件中的offset和length时，发现可以顺便计算出df，于是value修改为(df, offset, length)的三元组。至此我们已经可以利用term找出df, 然后用offset和length找出一级索引中需要的tf,id，并且这时tf还是从高到低排列的。

map输入：key为每行在TF文件中的起始位置(offset)， value为这一行的文本

map输出：key为这一行所代表的单词，value为其起始位置、长度组成的二元组

reduce输入：key为单词，value为组成元素为(offset, length)的列表

reduce输出：key为单词，value为(df, offset, length)的三元组。这里的df为列表元素的数目，也是TF文件中一个term占的行数，也相当于term的df值。offset为输入的列表中最小的offset，length为列表中length的和，两者用来指出TF文件中一个term所对应的部分的起始位置和长度以方便进行文件的读取。

文件大小为112MB。

2.4 POSITION文件(term_id_position)

map输入：key为该页面在xml中的起始位置offset，value为该页面的xml内容

map输出：key为(term,id)，value为该词在该文章中出现的位置(position0, position1, position2···)

在map中使用了HashMap<String, List<Long>>类来存放term和对应的position列表，输出时将这个page的id放入key中。

不需要reduce过程，文件大小为10.5GB。

3. 检索功能

在前期的试验过程中我先将从一个较小的xml文件(sample_enwiki.xml)中获得的id_title，term_id_position, term_df_offset_length文件存入mysql数据库，用户输入term后先在数据库中查找对应的df, offset, length，根据查到的offset和length从term_tf_id(也是较小的sample)文件中读取该词所对应的部分，也就是从高到低的tf值和对应的id。这时候可以选择查找标题功能使用id去查找标题，也可以使用查找位置功能去查找一个词在一篇文章中出现的位置。

前期试验成功后我将完整的term_df_offset_length文件(来自enwikisource-20171020-pages-articles-multistream.xml) 存入数据库，这时候就可以在完整的term_tf_id文件中读取数据了。对于term_id_position文件可以进行相同的操作，即对文件再次建立索引后使用offset和length来读取，由于时间关系和操作的相似性没有进行。
