package sparkscalademos.transformations

import org.apache.spark.SparkContext
import sparkscalademos._

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
object PairTransformationDemo extends App {
  val sc = new SparkContext(configure(getClass.getSimpleName))
  val pairs = (1 to 6) zip (11 to 16)
  val pairRDD = sc.parallelize(pairs) // ((1,11), (2,12), (3,13), (4,14), (5,15), (6,16))
  printRdd("Initial", pairRDD)

  val valueMapped = pairRDD.mapValues(_ * 2)
  printRdd("Map values example", valueMapped) // ((1,22), (2,24), (3,26), (4,28), (5,30), (6,32))

  sc.stop()
}
