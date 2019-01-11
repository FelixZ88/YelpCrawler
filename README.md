>Scrapy是一个非常强大的爬虫框架，只需极少代码便可应付一个简单爬虫。
>但如果需要几十万几百万的数据量，中间一旦有中断，重新爬取则太浪费时间。
>本文介绍一种思路，结合mysql，实现断点重爬的方式。

以国外美食网站Yelp为例，指定爬取香港的所有餐厅信息：餐厅名称、地址、评价等信息。

1. 任务分析
首先我们找到Yelp香港餐厅的列表页面，这个页面是我们的起始页面，对我们有用的信息为：餐厅列表和跳转页码。
![列表页面展示餐厅列表](https://upload-images.jianshu.io/upload_images/5241095-744edaf29f525cf1.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)
![餐厅列表和跳转页码](https://upload-images.jianshu.io/upload_images/5241095-1f90496ba935410d.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)
点击进入餐厅的详情页面，可以看到基本信息和用户评论，这里需要注意评论是区分语言的
![详情页面展示餐厅的详情及用户评论](https://upload-images.jianshu.io/upload_images/5241095-d7370ada5b52a443.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

2. Scrapy项目结构
```
.
├── README.md
└── yelp
    ├── __init__.py
    ├── scrapy.cfg
    └── yelp
        ├── __init__.py
        ├── items.py
        ├── middlewares.py
        ├── pipelines.py
        ├── settings.py
        └── spiders
            ├── YelpSpider.py
            └── __init__.py
```
这就是一个普通的Scrapy爬虫的项目结构，本文主要介绍如何实现断点续爬，所以如何获取详细字段内容的方式可以直接查看源码。

3. 断点续爬的实现
> 由于我们需要断点续爬，那么就必须要在某一个时刻记录当前的爬虫状态，哪些页面已经爬过，哪些还没有爬过。
> 我们将每一个需要爬取的页面抽象为一个爬虫任务，从该页面获取数据完成则认为该任务完成。设计这个任务的数据结构：

```
class TaskBean(base):
    __tablename__ = 'yelp_tasks'
    id = Column(Integer, primary_key=True) # 任务ID
    city = Column(String(256), nullable=False)
    url = Column(String(1024), nullable=False) # 爬虫任务的URL
    is_finished = Column(Boolean, default=False) # 标记该页面是否已经爬过
    parent_id = Column(Integer, default=-1) # 父任务的ID，这样所有任务形成一棵树
    type = Column(Integer)  # 任务类型，1. 页面列表任务 2. 餐厅详情任务 3. 评论任务

    def to_dict(self):
        return {'task_id': self.id, 'city': self.city, 'url': self.url, 'is_finished': self.is_finished, 'type': self.type}

```

这个数据结构中，我们定义了is_finished字段用来标记该任务是否已经完成
```
    is_finished = Column(Boolean, default=False) # 标记该页面是否已经爬过
```
并且定义了三种类型的任务
```
  TASK_TYPE_LIST = 1 # 页面列表任务
  TASK_TYPE_RESTAURANT = 2 # 餐厅详情任务
  TASK_TYPE_REVIEW = 3 # 评论任务
```
OK，任务已经定义完了，那怎么保存这个任务以及何时保存呢？
>考虑到爬虫任务随时可能中断，那么已爬取的数据可能随之丢失，我并没有没有用scrapy的pipeline和items来保存数据和任务进度，而是用了MySQL+sqlalchemy在同一个transaction中同时保存数据和任务进度。

下面的代码以爬取餐厅列表为例
```
        if task.type == TASK_TYPE_LIST:  # Restaurant list page
            # Get page url links
            page_tasks = []
            for page_url in response.selector.xpath('//*[@id="wrap"]/div[3]/div[2]/div[2]/div/div[1]/div[1]/div/div[1]/div/div[2]/div/div/a/@href').extract():
                page_url = response.urljoin(page_url)
                if page_url not in self.all_urls:
                    page_task = TaskBean()
                    page_task.city = city
                    page_task.url = page_url
                    page_task.type = TASK_TYPE_LIST
                    page_tasks.append(page_task)
                    self.all_urls.add(page_url)

            # Get restaurant links
            restaurant_tasks = []
            for restaurant_section in response.selector.xpath('//*[@id="wrap"]/div[3]/div[2]/div[2]/div/div[1]/div[1]/div/ul/li/div/div/div/div/div[2]/div[1]'):
                # name = restaurant_section.xpath('div[1]/div[1]/div[1]/h3/a/@title').extract_first()
                if restaurant_section.xpath('div[1]/div[1]/div[1]/h3/span').extract():
                    continue
                url = response.urljoin(restaurant_section.xpath('div[1]/div[1]/div[1]/h3/a/@href').extract_first())
                if url not in self.all_urls:
                    restaurant_task = TaskBean()
                    restaurant_task.url = url
                    restaurant_task.city = city
                    restaurant_task.type = TASK_TYPE_RESTAURANT
                    restaurant_tasks.append(restaurant_task)
                    self.all_urls.add(url)

            # 最后添加页面任务、餐厅详情页任务并将当前任务置位已完成，这些需要在同一个transaction中完成
            task.is_finished = True
            session.add(task)
            session.add_all(page_tasks)
            session.add_all(restaurant_tasks)
            session.commit()
```

4. 随机U-A反爬虫设置
```

# Crawl responsibly by identifying yourself (and your website) on the user-agent
YELP_USER_AGENT = ["Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; AcooBrowser; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0; Acoo Browser; SLCC1; .NET CLR 2.0.50727; Media Center PC 5.0; .NET CLR 3.0.04506)",
    "Mozilla/4.0 (compatible; MSIE 7.0; AOL 9.5; AOLBuild 4337.35; Windows NT 5.1; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
    "Mozilla/5.0 (Windows; U; MSIE 9.0; Windows NT 9.0; en-US)",
    "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Win64; x64; Trident/5.0; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 2.0.50727; Media Center PC 6.0)",
    "Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 1.0.3705; .NET CLR 1.1.4322)",
    "Mozilla/4.0 (compatible; MSIE 7.0b; Windows NT 5.2; .NET CLR 1.1.4322; .NET CLR 2.0.50727; InfoPath.2; .NET CLR 3.0.04506.30)",
    "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN) AppleWebKit/523.15 (KHTML, like Gecko, Safari/419.3) Arora/0.3 (Change: 287 c9dfb30)",
    "Mozilla/5.0 (X11; U; Linux; en-US) AppleWebKit/527+ (KHTML, like Gecko, Safari/419.3) Arora/0.6",
    "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.8.1.2pre) Gecko/20070215 K-Ninja/2.1.1",
    "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN; rv:1.9) Gecko/20080705 Firefox/3.0 Kapiko/3.0",
    "Mozilla/5.0 (X11; Linux i686; U;) Gecko/20070322 Kazehakase/0.4.5",
    "Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.8) Gecko Fedora/1.9.0.8-1.fc10 Kazehakase/0.5.6",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/535.20 (KHTML, like Gecko) Chrome/19.0.1036.7 Safari/535.20",
    "Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; fr) Presto/2.9.168 Version/11.52",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.11 TaoBrowser/2.0 Safari/536.11",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.71 Safari/537.1 LBBROWSER",
    "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; LBBROWSER)",
    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E; LBBROWSER)",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.84 Safari/535.11 LBBROWSER",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E)",
    "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; QQBrowser/7.0.3698.400)",
    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; SV1; QQDownload 732; .NET4.0C; .NET4.0E; 360SE)",
    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E)",
    "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1",
    "Mozilla/5.0 (iPad; U; CPU OS 4_2_1 like Mac OS X; zh-cn) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8C148 Safari/6533.18.5",
    "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:2.0b13pre) Gecko/20110307 Firefox/4.0b13pre",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:16.0) Gecko/20100101 Firefox/16.0",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11",
    "Mozilla/5.0 (X11; U; Linux x86_64; zh-CN; rv:1.9.2.10) Gecko/20100922 Ubuntu/10.10 (maverick) Firefox/3.6.10",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"]

```

```
# Enable or disable downloader middlewares
# See https://doc.scrapy.org/en/latest/topics/downloader-middleware.html
DOWNLOADER_MIDDLEWARES = {
    'scrapy.downloadermiddleware.useragent.UserAgentMiddleware': None,
    'dianping.middlewares.DianpingUserAgentMiddleware': 400,
}
```

```
class YelpUserAgentMiddleware(UserAgentMiddleware):
    '''
    设置User-Agent
    '''

    def __init__(self, user_agent):
        self.user_agent = user_agent

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            user_agent=crawler.settings.get('YELP_USER_AGENT')
        )

    def process_request(self, request, spider):
        agent = random.choice(self.user_agent)
        request.headers['User-Agent'] = agent
```
