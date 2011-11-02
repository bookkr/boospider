#!/usr/bin/python
#coding=utf8

import urllib
import re
import MySQLdb
import time
import sys
import socket
from sgmllib import SGMLParser

url_pre = "http://book.douban.com/subject/"
history=file("log.txt", "w")
warning=file("wrong.txt", "w")

#lower = 6061536; upper = 6100000
lower = 6014994; upper = 6050000

info_dict={"定价":"price", "目录":"catalog", "出版社":"publisher_id", "作者":"writer_id", "出版年":"release",
           "ISBN":"realisbn", "title":"title", "coverlink":"coverlink", "内容简介":"about", 
           "副标题":"subtitle", "页数":"page", "原作名":"origin_title", "译者":"translator_id", "装帧":"cover_id"}

refKey=["作者", "译者", "出版社", "装帧"]
refTbname=["bookkr_book_writer", "bookkr_book_translator", "bookkr_book_publisher", "bookkr_book_cover"]
refTbAttr=["writer_name", "name", "publisher_name", "kind"]
refTbRef=["writer_id", "id", "publisher_id", "no"]
refTbOther=[{"作者简介":"writer_info"}, {"译者简介":"info"}, {}, {}]

class DataGetter(SGMLParser):
    def __init__(self):
        self.state = 0
        self.substate = 0
        self.skip = 0
        self.information={"":""}
        self.key=""
        self.value=""
        SGMLParser.__init__(self)

    def start_div(self, attr):
        self.skip = 0
        for k,v in attr:
            if k=="id":
                if v=="info":
                    self.state = 1
                elif v=="mainpic":
                    self.state = 2
                elif re.search("short", v):
                    self.skip=1
                elif re.search("full", v):
                    self.skip = 0
            elif k=="class":
                if v=="related_info":
                    self.state=3
                elif v=="indent":
                    self.substate = 2
                elif self.state==3:
                    self.state=0
    
    def start_span(self, attr):
        if self.state==1 and len(attr)==1 and attr[0][0]=="class" and attr[0][1]=="pl":
            self.value+="|"
        elif self.state == 3 and self.substate==2 and len(attr)==1 and attr[0][0]=="class":
            if attr[0][1] == "short":
                self.skip = 1
            else:
                self.skip = 0

    def end_span(self):
        if self.state==3 and self.skip>0:
            self.skip = self.skip-1

    def start_h2(self, attr):
        if self.state==3:
            self.substate=1
            self.skip = 0
    
    def handle_entityref(self, ref):
        if self.value!="":
            self.value+="&%s;"%ref

    def handle_data(self, text):
        if text.isspace():
            return

        if self.state==1:
            self.value+=text.strip()

        elif self.state==3:
            if self.substate==1:
                self.key=text.strip()
                self.value=""
            elif self.substate==2 and self.skip==0:
                self.value+=text
        elif self.state==4:
            if len(text)>8 and text[-8:]=="(豆瓣)":
                self.value+=text[0:-8].strip()
            else:
                self.value+=text.strip()
            
    def start_a(self, attr):
        if self.state==2:
            link = [v for k,v in attr if k=='href'][0]
            if not re.search("update", link):
                self.information["coverlink"]=link

    def start_img(self, attr):
        if self.state==2:
            link = [v for k,v in attr if k=='src'][0]
            if "coverlink" not in self.information and (not re.search("default", link)):
                self.information["coverlink"]=link

    def start_title(self, attr):
        self.state=4
        self.text=""

    def end_div(self):
        if self.state==1:
            for i in self.value.split("|"):
                if len(i)>1:
                    pairs = i.split(":")
                    self.information[ pairs[0].strip() ] = pairs[1].strip()
            self.value=""
            self.state=0

        elif self.state==3:
            if self.substate!=0:
                if self.key!="" and self.value!="":
                    self.information[self.key]=self.value
                    self.substate=0
                    self.key=""
                    self.value=""
            else:
                self.state=0
    
    def end_title(self):
        self.information["title"]=self.value
        self.value=""
        self.state = 0


def check_url(url_source):
    status_code = url_source.getcode()
    if status_code == 403:
        print "Connection forbidden"
        sys.exit(0)
    if status_code/100!=2:
        return False
    if re.search( 'book', url_source.geturl() ):
        return True
    return False

class download():
    def __init__(self):
        self.conn = MySQLdb.connect(host="shizheng.gotoftp4.com", user="shizheng", passwd="xx", db="shizheng", charset="utf8")
        self.url_id = 0
        self.cnt=0
        
    def update_data(self, getter):
        try:
            self.conn.ping()
        except Exception,e1:
            print e1
            try:
                conn.close()
                self.conn = MySQLdb.connect(host="shizheng.gotoftp4.com", user="shizheng", passwd="xx", db="shizheng", charset="utf8")
            except Exception,e2:
                print e2
                sys.exit(0)

        cursor = self.conn.cursor()
        
        #for example: 
        #select writer_id, writer_info from bookkr_book_writer where writer_name = "****"
        #insert into bookkr_book_writer(writer_name, writer_info) values('xxxx', 'xxxxx')
        #key: 作者
        #tbName: bookkr_book_writer
        #tbAttr: writer_name
        #tbRef: writer_id
        #tbOther: {"作者简介":"writer_info"}
        def filter_data(key, tbName, tbAttr, tbRef, tbOther):
            if key not in getter.information:
                return
            attr1 = [i for i in tbOther if i in getter.information]
            otherAttr = ""
            otherValue= ""

            if len(attr1)>=1:
                otherAttr = "," + str.join(",", ["`"+tbOther[i]+"`" for i in attr1])
                otherValue="," + str.join(",", ["\'"+getter.information[i]+"\'" for i in attr1])

            sqlQuery = "select %s from %s where %s=%s" %(tbRef, tbName, tbAttr, "\'"+getter.information[key]+"\'")
            res_cnt = cursor.execute(sqlQuery)
            result = cursor.fetchall()

            if res_cnt==0:
                sqlInsert = "insert into %s(%s%s) values(%s%s)" %(tbName, tbAttr, otherAttr, 
                                                                  "\'"+getter.information[key]+"\'", otherValue)
                cursor.execute(sqlInsert)
                res_cnt = cursor.execute(sqlQuery)
                result = cursor.fetchall()
            elif res_cnt==1 and otherAttr!="":
                otherValue = str.join(",", ['`'+tbOther[i]+'`=\''+getter.information[i]+'\'' for i in attr1] )
                sqlUpdate = "update %s set %s where %s=\'%s\'"%(tbName, otherValue, tbRef, result[0][0])
                cursor.execute(sqlUpdate)
            
            if res_cnt==1:
                getter.information[key]=result[0][0]

        map(filter_data, refKey, refTbname, refTbAttr, refTbRef, refTbOther)

        sql_columns = str.join(",", ["`"+info_dict[i]+"`" for i in info_dict if i in getter.information])
        if len(sql_columns)>0:
            sql_values = str.join(",", [ "\'"+str(getter.information[i])+"\'" for i in info_dict if i in getter.information])
            sqlQuery = "insert into bookkr_book_lib (%s) values (%s)" %(sql_columns, sql_values)
            cursor.execute(sqlQuery)
            self.cnt=self.cnt+1
        cursor.close()

    def fetch_data(self):

        conn_count = 0
        while conn_count<10:
            try:
                url_source = urllib.urlopen( url_pre+str(self.url_id) )
                break
            except Exception,e:
                print e
                print "download fail.Try again"
                conn_count=conn_count+1

        if conn_count>=10:
            print "Fail after tried %d times" %(count)
            return


        if check_url(url_source):
            all_data = url_source.read().replace("<br/>", "\n")
            getter = DataGetter()
            getter.feed( all_data )
            
            if ("title" not in getter.information) or ("ISBN" not in getter.information):
                return
            for i in getter.information:
                getter.information[i] = getter.information[i].replace("\'", "\\\'").replace("\"", "\\\"")

            if "出版年" in getter.information:
                date = re.findall(r"\d+", getter.information["出版年"])
                if len(date)<3:
                    date.extend(["01" for i in range(3-len(date))])

                getter.information["出版年"] = str.join("-", date)
            
            if "目录" in getter.information:
                v = getter.information["目录"]
                res = re.search('· · · · · ·     \(收起\)', v)
                if res:
                    getter.information["目录"]=v[0:res.start()]

            self.update_data(getter)

    def run(self):
        global lower, upper

        while True:
            if lower>=upper:
                break
            else:
                self.url_id = lower
                lower = lower + 1
                
                print self.url_id

                try:
                    self.fetch_data()
                except Exception,e:
                    print e
                    warning.write(str(self.url_id)+"\n")
                    warning.flush()

            if self.cnt>=10:
                try:
                    self.conn.commit()
                    self.cnt=0
                    history.write(str(self.url_id)+"\n")
                    history.flush()
                except Exception,e:
                    print e
            time.sleep(0.5)
        
        self.conn.commit()
        self.conn.close()
        history.write(str(self.url_id)+"\n")
        history.close()

if __name__=="__main__":
    socket.setdefaulttimeout(20)
    d = download()
    d.run()

    print "ok"

