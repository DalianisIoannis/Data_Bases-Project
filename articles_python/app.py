# ----- CONFIGURE YOUR EDITOR TO USE 4 SPACES PER TAB ----- #
import sys, os
sys.path.append(os.path.join(os.path.split(os.path.abspath(__file__))[0], 'lib'))
import pymysql as db
import settings


def connection():
    ''' User this function to create your connections '''
    con = db.connect(
        settings.mysql_host, 
        settings.mysql_user, 
        settings.mysql_passwd, 
        settings.mysql_schema)
    return con


def classify(topn):
    # Create a new connection
    con=connection()
    # Create a cursor on the connection
    cur=con.cursor()
    try:
        topn_int = int(topn)
    except:
        print('wrong')
    #find the articles 
    sqlQuery = """SELECT DISTINCT (articles.title),articles.summary
                FROM
                    articles
                WHERE
                    NOT EXISTS( SELECT *
                FROM
                    article_has_class
                WHERE
                    article_has_class.articles_id = articles.id);"""
    try:
   # Execute the SQL command
        cur.execute(sqlQuery)
    except:
        print ("Error: unable to fetch data")
    # Fetch all the rows in a list of lists.
    results = cur.fetchall()
    arthro_list=list()#[("title","class","subclass", "weightsum"),]
    #for each article
    for row in results:
        article_name = row[0]
        artcicle_summary = row[1]#summary
        list_of_terms = artcicle_summary.split()#all the words of the summary
        total_class_subclass=list()
        for w in list_of_terms :# for each word
            #must fetch all classes and subclasses and then sum the weights
            sqlQuery = """ SELECT class , subclass 
                            FROM classes
                            WHERE term='%s'""" % (w)
            try:
                # Execute the SQL command
                cur.execute(sqlQuery)
            except:
                print ("Error: unable to fetch data")
   
            # Fetch all the rows in a list of lists.
            results2 = cur.fetchall()# classes and subclasses for each word
            
            for index_results2 in results2:
                total_class_subclass.append(index_results2)

        #for every category
        for row2 in total_class_subclass:
            category_class = row2[0]
            category_subclass = row2[1]
            sum_weight=0
            for row3 in list_of_terms:
                sqlQuery = """ SELECT weight 
                                FROM classes
                                WHERE class='%s' and subclass='%s'
                                and term='%s'"""% (category_class,category_subclass,row3)
                try:
                # Execute the SQL command
                    cur.execute(sqlQuery)
                except:
                    print ("Error: unable to fetch data ")
                results3 = cur.fetchall()
                global term_weight
                for gtxs in results3:
                    #global term_weight
                    term_weight=0
                    if len(results3)!=0:
                        term_weight=float(gtxs[0])
                        sum_weight=sum_weight+term_weight

            a = [article_name,category_class,category_subclass,sum_weight]
            arthro_list.append(a)

    arthro_list.sort(key=lambda x: x[3] , reverse = True)
    noduplist = list()
    for line in arthro_list:
        if line not in noduplist:
            noduplist.append(line)
    result_list=list()
    for roww in results:
        compare_to_N = 0 
        article_name_to_indicate=roww[0]
        for line in noduplist:
            if line[0]==article_name_to_indicate and compare_to_N<topn_int:
                compare_to_N=compare_to_N+1
                result_list.append(line)
    result_list.insert(0 , ("title","class","subclass", "weightsum"), )
    cur.close()
    con.close()
    return(result_list)

def updateweight(class1,subclass1,weight):
   # Create a new connection
    con=connection()
    # Create a cursor on the connection
    cur=con.cursor()
    try:
        x_int = float(weight)
    except:
        print('Error: unable to fetch data')
    sql = """ SELECT EXISTS
                (SELECT * FROM classes 
                WHERE CLASS = '%s' AND SUBCLASS = '%s' 
                AND WEIGHT>'%f')""" % (class1,subclass1,x_int);
    try:
        # Execute the SQL command
        cur.execute(sql)
    except:
        print ("Error: unable to fetch data ")
    results = cur.fetchall()
    for gtxs in results:
        number=gtxs[0]
        if number==0:
            return[("result",),("error",)]
    sql = """UPDATE classes 
            SET weight = weight +Abs((1/2)*(weight-'%d')) 
            WHERE class = '%s' AND subclass = '%s' 
            AND weight>'%f'""" % (x_int,class1,subclass1,x_int)
    try:
        # Execute the SQL command
        cur.execute(sql)
    except:
        # Rollback in case there is any error
        con.rollback()

    # Commit your changes in the database
    con.commit()
    cur.close()
    con.close()
    return[("result",),("ok",)]
    
	
def selectTopNClasses(fromdate, todate,n):
    # Create a new connection
    con=connection()
    # Create a cursor on the connection
    cur=con.cursor()
    try:
        n_int = int(n)
    except:
        print('Error: unable to fetch data')
    sqlQuery = """select classes.class , classes.subclass , count( distinct (articles.id)) 
                    from articles , article_has_class , classes 
                    where articles.date>='%s' and articles.date<='%s' 
                            and article_has_class.articles_id=articles.id 
                            and classes.class=article_has_class.class 
                            and classes.subclass=article_has_class.subclass 
                            group by classes.class , classes.subclass 
                            order by count(distinct articles.id) DESC 
                            limit %d""" %(fromdate,todate,n_int)
    try:
   # Execute the SQL command
        cur.execute(sqlQuery)
    except:
        print ("Error: unable to fetch data")
    # Fetch all the rows in a list of lists.
    results = cur.fetchall()
    result = list()# [("class","subclass", "count"),]
    for row in results:
        result.append(row)
    result.insert(0 , ("class","subclass", "count"), )
    cur.close()
    con.close()
    return result

def countArticles(class1,subclass1):
    # Create a new connection
    con=connection()
    # Create a cursor on the connection
    cur=con.cursor()
    
    sqlQuery = """ 
        SELECT 
            COUNT(DISTINCT (articles_id))
        FROM
            article_has_class
        WHERE
            class = '%s' AND subclass = '%s'
        GROUP BY class , subclass; """ %(class1 , subclass1)
    try:
   # Execute the SQL command
        cur.execute(sqlQuery)
    except:
        print ("Error: unable to fetch data")
        
    results = cur.fetchall()
    result = [("count",),]
    for row in results:
        result.append(row)
    cur.close()
    con.close()
    return result


def JaccardNew(temp_list,temp2_list):
    #commonlist=list()
    #for w in temp_list:
    #    if w in temp2_list:
    #        if w not in commonlist:
    #            commonlist.append(w)
    #union = len(commonlist)
    #b = len(temp_list)+len(temp2_list)-union
    #return [(float)(union/b)]
#######################################################
    words1=set(temp_list)
    words2=set(temp2_list)
    commonlist=words1.intersection(words2)
    return float( len(commonlist) ) / float( len(words1) + len(words2) - len(commonlist) )
#######################################################

def findSimilarArticles(articleId,n):
    # Create a new connection
    con=connection()
    # Create a cursor on the connection
    cur=con.cursor()
    
    sqlQuery = """SELECT articles.summary
                FROM
                    articles
                WHERE
                     articles.id = '%s';""" % (articleId)

    try:
   # Execute the SQL command
        cur.execute(sqlQuery)
    except:
        print ("Error: unable to fetch data ")

    results = cur.fetchall()
    temp_list=list()
    for row in results:
        summar = row[0]
        temp_list=summar.split()
        sqlQuery = """SELECT 
                            articles.summary , articles.id , articles.title
                        FROM 
                            articles
                        WHERE
                            articles.id != '%s';""" %(articleId)
        try:
            cur.execute(sqlQuery)
        except:
            print ("Error: unable to fetch data ")
        results2 = cur.fetchall()
        
        temp2_list=list()        
        ultimateresults = list()
        for row2 in results2:
            summary2 = row2[0]
            name2 = row2[1]
            title2 = row2[2]
            temp2_list=summary2.split()
            value = JaccardNew(temp_list,temp2_list)
            a = [name2 , value , title2]
            ultimateresults.append(a)    
        ultimateresults.sort(key=lambda x: x[1] , reverse = True)
        #ultimateresults.insert(0 , ("Article ID","Article Title"))
        try:
            n_int = int(n)
        except:
            print("Error: unable to fetch data ")
        return_list=list()
        for counter in ultimateresults:
            article_id_final = counter[0]
            article_title_final = counter[2]
            b = [ article_id_final , article_title_final ]
            return_list.append(b)
    return_list.insert(0 , ("Article ID","Article Title"))
    cur.close()
    con.close()
    return return_list[:(n_int+1)]
    #return ultimateresults[:(n_int+1)]

 