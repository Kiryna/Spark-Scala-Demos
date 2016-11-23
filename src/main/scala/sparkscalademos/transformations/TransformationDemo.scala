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
object TransformationDemo extends App {
  val conf = configure(getClass.getSimpleName)
  val sc = new SparkContext(conf)
  val rdd = sc.parallelize(1 to 10) // (1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
  printRdd("Initial", rdd)

  val mappedRdd = rdd.map(_ * 2)
  printRdd("Map example", mappedRdd) // (2, 4, 6, 8, 10, 12, 14, 16, 18, 20)

  val filteredRdd = rdd.filter(_ % 2 == 0) //(2, 4, 6, 8, 10)
  printRdd("Filter example", filteredRdd)

  val flatMappedRdd = rdd.flatMap(el => 0 to el) // (0, 1, 0, 1, 2, 0, 1, 2, 3, 0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 5 ... 10)
  printRdd("Flat map example", flatMappedRdd)

  val distinctRdd = flatMappedRdd.distinct() // (4, 0, 1, 6, 3, 7, 9, 8, 10, 5, 2)
  printRdd("distinct example", distinctRdd)

  sc.stop()
}
