package sparkscalademos.configurations

import org.apache.spark.SparkConf

/*
 * Copyright (C) 2016 Iryna Kharaborkina.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
object ConfigurationsDemo extends App {
  val conf = new SparkConf().
    setMaster("local").                       // (spark.master   -> local)
    setAppName("Spark Scala Demos").          // (spark.app.name -> Spark Scala Demos)
    set("spark.ui.port", "4041")              // (spark.ui.port  -> 4041)

  conf.getAll.foreach(println)
}
