import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.rdd.RDD
import scala.math.{log,min}

object Test{

    def cal_s(ls:List[Char]):Double = {
        val M:Map[Char,Int] = ls.map((_,1)).groupBy(_._1).map{ case(k,v)=>(k,v.map(_._2).reduce(_+_)) }    
        val count:Int = ls.length
        M.values.foldLeft(0.0)((s,elem)=>s-elem*1.0/count*log(elem*1.0/count))
    }

    def inner_word2(
            word1:RDD[(Char,Int)],
            word2:RDD[(String,Int)],
            length:Int):RDD[(String,Double)] = {
        val w1 = word2.map( {case(k,v)=>(k(0),k)} )
        val w2 = word2.map( {case(k,v)=>(k(1),k)} )

        val w1_word1 = w1.join(word1).map( {case(k,(v1,v2))=>(v1,v2)} )
        val w2_word1 = w2.join(word1).map( {case(k,(v1,v2))=>(v1,v2)} )
        val w1_w2 = w1_word1.join(w2_word1).map( {case(k,(v1,v2))=>(k,v1*v2)} )
        val inner_ret = word2.join(w1_w2).map( {case(k,(v1,v2))=>(k,v1*1.0*length/v2)} ).filter( {case(k,v)=>v>30} )

        inner_ret
    }

    def inner_word3(
            word1:RDD[(Char,Int)],
            word2:RDD[(String,Int)],
            word3:RDD[(String,Int)],
            length:Int):RDD[(String,Double)] = {
        val w12_1 = word3.map( {case(k,v)=>(k(0),k)} ).join(word1).map( {case(k,(v1,v2))=>(v1,v2)} )
        val w12_2 = word3.map( {case(k,v)=>(k.substring(1),k)} ).join(word2).map( {case(k,(v1,v2))=>(v1,v2)} )
        val w21_2 = word3.map( {case(k,v)=>(k.substring(0,2),k)} ).join(word2).map( {case(k,(v1,v2))=>(v1,v2)} )
        val w21_1 = word3.map( {case(k,v)=>(k(2),k)} ).join(word1).map( {case(k,(v1,v2))=>(v1,v2)} )
        
        val w12 = w12_1.join(w12_2).map( {case(k,(v1,v2))=>(k,v1*v2)} )
        val w21 = w21_2.join(w21_1).map( {case(k,(v1,v2))=>(k,v1*v2)} )

        val tmp12 = word3.join(w12).map( {case(k,(v1,v2))=>(k,v1*1.0*length/v2)} ).filter( {case(k,v)=>v>30} )
        val tmp21 = word3.join(w21).map( {case(k,(v1,v2))=>(k,v1*1.0*length/v2)} ).filter( {case(k,v)=>v>30} )

        tmp12.join(tmp21).map( {case(k,(v1,v2))=>(k,min(v1,v2))} )
    }

    def inner_word4(
            word1:RDD[(Char,Int)],
            word2:RDD[(String,Int)],
            word3:RDD[(String,Int)],
            word4:RDD[(String,Int)],
            length:Int):RDD[(String,Double)] = {
        val w13_1 = word4.map( {case(k,v)=>(k(0),k)} ).join(word1).map( {case(k,(v1,v2))=>(v1,v2)} )
        val w13_3 = word4.map( {case(k,v)=>(k.substring(1),k)} ).join(word3).map( {case(k,(v1,v2))=>(v1,v2)} )
        val w22_1 = word4.map( {case(k,v)=>(k.substring(0,2),k)} ).join(word2).map( {case(k,(v1,v2))=>(v1,v2)} )
        val w22_2 = word4.map( {case(k,v)=>(k.substring(2,4),k)} ).join(word2).map( {case(k,(v1,v2))=>(v1,v2)} )
        val w31_3 = word4.map( {case(k,v)=>(k.substring(0,3),k)} ).join(word3).map( {case(k,(v1,v2))=>(v1,v2)} )
        val w31_1 = word4.map( {case(k,v)=>(k(3),k)} ).join(word1).map( {case(k,(v1,v2))=>(v1,v2)} )

        val w13 = w13_1.join(w13_3).map( {case(k,(v1,v2))=>(k,v1*v2)} )
        val w22 = w22_1.join(w22_2).map( {case(k,(v1,v2))=>(k,v1*v2)} )
        val w31 = w31_3.join(w31_1).map( {case(k,(v1,v2))=>(k,v1*v2)} )

        val tmp13 = word4.join(w13).map( {case(k,(v1,v2))=>(k,v1*1.0*length/v2)} ).filter( {case(k,v)=>v>30} )
        val tmp22 = word4.join(w22).map( {case(k,(v1,v2))=>(k,v1*1.0*length/v2)} ).filter( {case(k,v)=>v>30} )
        val tmp31 = word4.join(w31).map( {case(k,(v1,v2))=>(k,v1*1.0*length/v2)} ).filter( {case(k,v)=>v>30} )

        tmp13.join(tmp22).map( {case(k,(v1,v2))=>(k,min(v1,v2))} ).join(tmp31).map( {case(k,(v1,v2))=>(k,min(v1,v2))} )
    }

    def outer_word2(
            text:RDD[String]):RDD[(String,Double)] = {
        val left_word = text.filter(_.length>1).flatMap(
            line=>("^(..)".r findAllIn line).map((_,'#')).toList
                    :::("^(..)".r findAllIn line.substring(1)).map((_,'#')).toList
                    :::("(.)(..)".r findAllIn line).map(word=>(word.substring(1),word(0))).toList
                    :::("(.)(..)".r findAllIn line.substring(1)).map(word=>(word.substring(1),word(0))).toList
        ).groupByKey().map( {case(k,ls)=>(k,cal_s(ls.toList))} )
        val right_word = text.filter(_.length>1).flatMap(
            line=>("(..)$".r findAllIn line).map((_,'#')).toList
                    :::("(..)$".r findAllIn line.substring(1)).map((_,'#')).toList
                    :::("(..)(.)".r findAllIn line).map(word=>(word.substring(0,2),word(2))).toList
                    :::("(..)(.)".r findAllIn line.substring(1)).map(word=>(word.substring(0,2),word(2))).toList
        ).groupByKey().map( {case(k,ls)=>(k,cal_s(ls.toList))} )

        left_word.join(right_word).filter( {case(k,(v1,v2))=>v1>2.0 && v2>2.0} ).map( {case(k,(v1,v2))=>(k,min(v1,v2))} )
    }
        
    def outer_word3(
            text:RDD[String]):RDD[(String,Double)] = {
        val left_word = text.filter(_.length>2).flatMap(
            line=>("^(...)".r findAllIn line).map((_,'#')).toList
                    :::("^(...)".r findAllIn line.substring(1)).map((_,'#')).toList
                    :::("^(...)".r findAllIn line.substring(2)).map((_,'#')).toList
                    :::("(.)(...)".r findAllIn line).map(word=>(word.substring(1),word(0))).toList
                    :::("(.)(...)".r findAllIn line.substring(1)).map(word=>(word.substring(1),word(0))).toList
                    :::("(.)(...)".r findAllIn line.substring(2)).map(word=>(word.substring(1),word(0))).toList
        ).groupByKey().map( {case(k,ls)=>(k,cal_s(ls.toList))} )
        val right_word = text.filter(_.length>2).flatMap(
            line=>("(...)$".r findAllIn line).map((_,'#')).toList
                    :::("(...)$".r findAllIn line.substring(1)).map((_,'#')).toList
                    :::("(...)$".r findAllIn line.substring(2)).map((_,'#')).toList
                    :::("(...)(.)".r findAllIn line).map(word=>(word.substring(0,3),word(3))).toList
                    :::("(...)(.)".r findAllIn line.substring(1)).map(word=>(word.substring(0,3),word(3))).toList
                    :::("(...)(.)".r findAllIn line.substring(2)).map(word=>(word.substring(0,3),word(3))).toList
        ).groupByKey().map( {case(k,ls)=>(k,cal_s(ls.toList))} )

        left_word.join(right_word).filter( {case(k,(v1,v2))=>v1>2.0 && v2>2.0} ).map( {case(k,(v1,v2))=>(k,min(v1,v2))} )
    }

    def outer_word4(
            text:RDD[String]):RDD[(String,Double)] = {
        val left_word = text.filter(_.length>3).flatMap(
            line=>("^(....)".r findAllIn line).map((_,'#')).toList
                    :::("^(....)".r findAllIn line.substring(1)).map((_,'#')).toList
                    :::("^(....)".r findAllIn line.substring(2)).map((_,'#')).toList
                    :::("^(....)".r findAllIn line.substring(3)).map((_,'#')).toList
                    :::("(.)(....)".r findAllIn line).map(word=>(word.substring(1),word(0))).toList
                    :::("(.)(....)".r findAllIn line.substring(1)).map(word=>(word.substring(1),word(0))).toList
                    :::("(.)(....)".r findAllIn line.substring(2)).map(word=>(word.substring(1),word(0))).toList
                    :::("(.)(....)".r findAllIn line.substring(3)).map(word=>(word.substring(1),word(0))).toList
        ).groupByKey().map( {case(k,ls)=>(k,cal_s(ls.toList))} )
        val right_word = text.filter(_.length>3).flatMap(
            line=>("(....)$".r findAllIn line).map((_,'#')).toList
                    :::("(....)$".r findAllIn line.substring(1)).map((_,'#')).toList
                    :::("(....)$".r findAllIn line.substring(2)).map((_,'#')).toList
                    :::("(....)$".r findAllIn line.substring(3)).map((_,'#')).toList
                    :::("(....)(.)".r findAllIn line).map(word=>(word.substring(0,4),word(4))).toList
                    :::("(....)(.)".r findAllIn line.substring(1)).map(word=>(word.substring(0,4),word(4))).toList
                    :::("(....)(.)".r findAllIn line.substring(2)).map(word=>(word.substring(0,4),word(4))).toList
                    :::("(....)(.)".r findAllIn line.substring(3)).map(word=>(word.substring(0,4),word(4))).toList
        ).groupByKey().map( {case(k,ls)=>(k,cal_s(ls.toList))} )

        left_word.join(right_word).filter( {case(k,(v1,v2))=>v1>2.0 && v2>2.0} ).map( {case(k,(v1,v2))=>(k,min(v1,v2))} )
    }
            
    def main(args:Array[String]) = {
        val sparkconf = new SparkConf().setAppName("Test")
        val sc = new SparkContext(sparkconf)

        val drop_list = Array("，","\n","。","、","：",",","(",")","]","[","."," ","“","”","？","?","！","·","!","‘","’","…","；",";","#")
        val src_text = sc.textFile("/user/hztengkezhen/h35/content",10)
        val text = drop_list.foldLeft(src_text)((src_text,word)=>src_text.map(line=>line.replace(word,"")))

        val length = text.map(line=>line.length).reduce(_+_)

        val word1 = text.flatMap(_.toList).map((_,1)).reduceByKey(_+_).cache()
        val word2 = text.filter(_.length>1).flatMap(line=>("(..)".r findAllIn line).toList:::("(..)".r findAllIn line.substring(1)).toList).map((_,1)).reduceByKey(_+_).cache()
        val word3 = text.filter(_.length>2).flatMap(
                        line=>("(...)".r findAllIn line).toList:::
                        ("(...)".r findAllIn line.substring(1)).toList:::
                        ("(...)".r findAllIn line.substring(2)).toList
        ).map((_,1)).reduceByKey(_+_).cache()
        val word4 = text.filter(_.length>3).flatMap(
                        line=>("(....)".r findAllIn line).toList:::
                        ("(....)".r findAllIn line.substring(1)).toList:::
                        ("(....)".r findAllIn line.substring(2)).toList:::
                        ("(....)".r findAllIn line.substring(3)).toList
        ).map((_,1)).reduceByKey(_+_).cache()

//for word2
        val inner_ret2 = inner_word2(word1,word2,length)
        val outer_ret2 = outer_word2(text)
        val ret2 = inner_ret2.join(outer_ret2).map( {case(k,v)=>(k,1)} ).join(word2).map( {case(k,v)=>(k,v._2)} )
        ret2.map(line=>Array(line._1.toString,line._2.toString,2.toString).mkString("\t")).collect().foreach(println)

//for word3
        val inner_ret3 = inner_word3(word1,word2,word3,length)
        val outer_ret3 = outer_word3(text)
        val ret3 = inner_ret3.join(outer_ret3).map( {case(k,v)=>(k,1)} ).join(word3).map( {case(k,v)=>(k,v._2)} )
        ret3.map(line=>Array(line._1.toString,line._2.toString,3.toString).mkString("\t")).collect().foreach(println)

//for word4
        val inner_ret4 = inner_word4(word1,word2,word3,word4,length)
        val outer_ret4 = outer_word4(text)
        val ret4 = inner_ret4.join(outer_ret4).map( {case(k,v)=>(k,1)} ).join(word4).map( {case(k,v)=>(k,v._2)} )
        ret4.map(line=>Array(line._1.toString,line._2.toString,4.toString).mkString("\t")).collect().foreach(println)

        sc.stop()
    }
}
