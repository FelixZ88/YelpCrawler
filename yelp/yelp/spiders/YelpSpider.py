
import scrapy
import unicodedata
from scrapy.spiders.crawl import CrawlSpider
from .. import session
from .. import TaskBean, RestaurantBean, RestaurantReviewBean
import json
import re
from .. import logger


TASK_TYPE_LIST = 1
TASK_TYPE_RESTAURANT = 2
TASK_TYPE_REVIEW = 3


class YelpSpider(CrawlSpider):

    name = 'yelp'

    # task filter
    all_urls = set()

    @property
    def get_start_tasks(self):
        return [
            # Here is the city which we gonna crawl
            {'city': 'HongKong', 'url': 'https://www.yelp.com/search?cflt=restaurants&find_loc=Hong+Kong'},
        ]

    # Everything starts from here
    def start_requests(self):
        tasks = session.query(TaskBean).all()
        if len(tasks) == 0:
            new_tasks = []
            for task in self.get_start_tasks:
                new_task = TaskBean()
                new_task.url = task['url']
                new_task.city = task['city']
                new_task.type = TASK_TYPE_LIST
                new_tasks.append(new_task)
                self.all_urls.add(new_task.url)

            session.add_all(new_tasks)
            session.commit()
        else:
            for task in tasks:
                self.all_urls.add(task.url)

        # Read all exist unfinished tasks
        unfinished_tasks = session.query(TaskBean).filter_by(is_finished=False).all()
        for task in unfinished_tasks:
            yield scrapy.Request(url=task.url, callback=self.parse_main, meta=task.to_dict())

    def parse_main(self, response):
        task_id = response.meta['task_id']
        city = response.meta['city']
        task = session.query(TaskBean).filter_by(id=task_id).first()
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

            # Label current task is finished and save all new tasks to db
            task.is_finished = True
            session.add(task)
            session.add_all(page_tasks)
            session.add_all(restaurant_tasks)
            session.commit()

            for page in page_tasks:
                dict = page.to_dict()
                yield scrapy.Request(url=page.url, callback=self.parse_main, meta=dict)

            for restaurant_task in restaurant_tasks:
                yield scrapy.Request(url=restaurant_task.url, callback=self.parse_main, meta=restaurant_task.to_dict())

        elif task.type == TASK_TYPE_RESTAURANT:  # Restaurant main page
            try:
                restaurant = RestaurantBean()
                restaurant.city = response.meta['city']
                restaurant.url_restaurant = response.url
                restaurant.task_id = task_id
                restaurant_name_section = response.selector.xpath('//*[starts-with(@class, "biz-page-header-left")]')
                restaurant.restaurant_name = restaurant_name_section.xpath('//*/h1/text()').extract_first().strip()
                try:
                    alter_names = '|'.join(restaurant_name_section.xpath('//*[starts-with(@class, "alternate-names")]/text()').extract()).strip()
                    restaurant.alter_names = alter_names
                except Exception as e:
                    logger.error("Exception alter_names {} at {}".format(e, response.url))
                try:
                    div_biz_rating = response.selector.xpath('//*[starts-with(@class, "biz-rating biz-rating-very-large")]')
                    star = div_biz_rating.xpath('*[starts-with(@class, "i-stars")]/@title').extract_first()
                    star = re.findall(r"([0-9]{1,}[.][0-9]*)", star)[0]
                    restaurant.star = int(float(star) * 10)
                    star_detail = '|'.join(response.selector.xpath('//*[@class="histogram_count"]/text()').extract())
                    restaurant.star_detail = star_detail
                    reviews = div_biz_rating.xpath('span/text()').extract_first()
                    reviews = re.findall(r"([0-9]+)", reviews.strip())[0]
                    restaurant.count_reviews = int(reviews)
                except Exception as e:
                    logger.error("Exception star,reviews {} at {}".format(e, response.url))
                try:
                    category = '|'.join(response.selector.xpath('//*[starts-with(@class, "biz-page-header-left")]//*[@class="category-str-list"]/a/text()').extract())
                    restaurant.category = category
                except Exception as e:
                    logger.error("Exception category {} at {}".format(e, response.url))

                try:
                    address = " ".join(response.selector.xpath('//*[@class="street-address"]/address/text()').extract()).strip()
                    restaurant.address = address
                except Exception as e:
                    logger.error("Exception address {} at {}".format(e, response.url))
                try:
                    phone = response.selector.xpath('//*[@id="wrap"]//*[@class="biz-phone"]/text()').extract_first().strip()
                    restaurant.phone = phone
                except Exception as e:
                    logger.error("Exception phone {} at {}".format(e, response.url))
                try:
                    map_url = response.selector.xpath('//*[@class="biz-map-directions"]/img/@src').extract_first()
                    from urllib import parse
                    url_para = parse.parse_qs(map_url)
                    latitude, langitude = url_para['center'][0].split(',')
                    restaurant.latitude, restaurant.langitude = latitude, langitude
                except Exception as e:
                    logger.error("Exception map_url {} at {}".format(e, response.url))
                try:
                    restaurant_host_url = response.selector.xpath('//*[starts-with(@class, "biz-website")]/a/@href').extract_first()
                    restaurant_host_url = response.urljoin(restaurant_host_url)
                    url_para = parse.parse_qs(restaurant_host_url)
                    for k in url_para.keys():
                        if k.startswith('https://www.yelp.com'):
                            restaurant_host_url = url_para[k][0]
                            restaurant.url_host = restaurant_host_url
                            break
                except Exception as e:
                    logger.error("Exception url_host {} at {}".format(e, response.url))

                try:
                    reviews = []
                    # get reviews
                    review_sections = response.selector.xpath('//*[@class="review-content"]')
                    for review_section in review_sections:
                        review = RestaurantReviewBean()
                        star = review_section.xpath('div/div/div/@title').extract_first().strip()
                        star = int(float(re.findall(r"([0-9]{1,}[.][0-9]*)", star)[0]) * 10)
                        date = review_section.xpath('*/span/text()').extract_first().strip()
                        content = "\n".join([d.strip() for d in review_section.xpath('p/text()').extract()])
                        language = review_section.xpath('p/@lang').extract_first()
                        if review_section.xpath('ul/li/div/a/@href'):
                            images = ' '.join([response.urljoin(url) for url in review_section.xpath('ul/li/div/img/@src').extract()])
                            review.images = images
                        from datetime import datetime
                        d_date = datetime.strptime(date, '%m/%d/%Y')
                        review.task_id = task_id
                        review.star = star
                        review.date = d_date
                        review.content = bytes(content, encoding="utf8")
                        review.language = language
                        reviews.append(review)
                except Exception as e:
                    logger.error("Exception reviews {} at {}".format(e, response.url))

                try:
                    review_tasks = []
                    # different language reviews
                    other_language_reviews = response.selector.xpath('//*[@class="feed"]//*[contains(@class, "feed_language")]//*[@class="dropdown_item"]/a/@href').extract()
                    for language_review in other_language_reviews:
                        if language_review not in self.all_urls:
                            review_task = TaskBean()
                            review_task.city = task.city
                            review_task.url = language_review
                            review_task.type = TASK_TYPE_REVIEW
                            review_task.parent_id = task.id
                            review_tasks.append(review_task)
                            self.all_urls.add(language_review)
                except Exception as e:
                    logger.error("Exception other_language_reviews {} at {}".format(e, response.url))

                # Label current task is finished and save all new tasks to db
                restaurant.reviews = reviews
                task.is_finished = True
                session.add(restaurant)
                session.add(task)
                session.add_all(reviews)
                session.add_all(review_tasks)
                session.commit()

                for review_task in review_tasks:
                    yield scrapy.Request(url=review_task.url, callback=self.parse_main, meta=review_task.to_dict())

            except Exception as e:
                logger.error("Exception cannot save info {} at {}".format(e, response.url))

        elif task.type == TASK_TYPE_REVIEW:  # Reviews page
            try:
                parent_task_id = task.parent_id
                restaurant = session.query(RestaurantBean).filter_by(task_id=parent_task_id).first()
                try:
                    reviews = []
                    review_sections = response.selector.xpath('//*[@class="review-content"]')
                    for review_section in review_sections:
                        review = RestaurantReviewBean()
                        star = review_section.xpath('div/div/div/@title').extract_first().strip()
                        star = int(float(re.findall(r"([0-9]{1,}[.][0-9]*)", star)[0]) * 10)
                        date = review_section.xpath('*/span/text()').extract_first().strip()
                        content = "\n".join([d.strip() for d in review_section.xpath('p/text()').extract()])
                        language = review_section.xpath('p/@lang').extract_first()
                        # review contains images
                        if review_section.xpath('ul/li/div/a/@href'):
                            images = ' '.join([response.urljoin(url) for url in review_section.xpath('ul/li/div/img/@src').extract()])
                            review.images = images
                        from datetime import datetime
                        # decode different datetime format
                        if language == 'ja' or language == 'zh':
                            d_date = datetime.strptime(date, '%Y/%m/%d')
                        elif language == 'es' or language == 'fr' or language == 'it' or language == 'pt':
                            d_date = datetime.strptime(date, '%d/%m/%Y')
                        elif language == 'de' or language == 'nb' or language == 'tr' or language == 'fi' or language == 'da':
                            d_date = datetime.strptime(date, '%d.%m.%Y')
                        elif language == 'sv':
                            d_date = datetime.strptime(date, '%Y-%m-%d')
                        elif language == 'pl' or language == 'nl':
                            d_date = datetime.strptime(date, '%d-%m-%Y')
                        else:
                            d_date = datetime.strptime(date, '%m/%d/%Y')
                        review.task_id = task_id
                        review.star = star
                        review.date = d_date
                        review.content = bytes(content, encoding="utf8")
                        review.language = language
                        review.restaurant_id = restaurant.id
                        reviews.append(review)

                    # Label current task is finished and save all new tasks to db
                    task.is_finished = True
                    session.add(restaurant)
                    session.add(task)
                    session.add_all(reviews)
                    session.commit()
                except Exception as e:
                    logger.error("Exception reviews {} at {}".format(e, response.url))

            except Exception as e:
                logger.error("Exception when TASK_TYPE_REVIEW cannot save info {} at {}".format(e, response.url))


