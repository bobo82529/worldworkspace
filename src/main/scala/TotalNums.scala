object TotalNums {
  //今日里程
  def totalMileage(totalMileage: Int, mileage: Int): Int = {
    var totalMileageNew = totalMileage + mileage
    totalMileageNew
  }

  //今日急加速次数
  def totalAccelerationRapidTimes(totalAccelerationRapidTimes: Int, accelerationRapidTimes: Int): Int = {
    var totalAccelerationRapidTimesNew = totalAccelerationRapidTimes + accelerationRapidTimes
    totalAccelerationRapidTimesNew
  }

  //今日急减速次数
  def totalDecelerationRapidTimes(totalDecelerationRapidTimes: Int, decelerationRapidTimes: Int): Int = {
    var totalDecelerationRapidTimesNew = totalDecelerationRapidTimes + decelerationRapidTimes
    totalDecelerationRapidTimesNew
  }

  //今日急转弯次数
  def totalSuddenTurnTimes(totalSuddenTurnTimes: Int, suddenTurnTimes: Int): Int = {
    var totalSuddenTurnTimesNew = totalSuddenTurnTimes + suddenTurnTimes
    totalSuddenTurnTimesNew
  }

  //今日三急次数
  def totalThreeTimes(totalAccelerationRapidTimes: Int, totalDecelerationRapidTimes: Int, totalSuddenTurnTimes: Int): Int = {
    var totalThreeTimesNew = totalAccelerationRapidTimes + totalDecelerationRapidTimes + totalSuddenTurnTimes
    totalThreeTimesNew
  }

  def scoreCount(score: Int, scoreCountArray: Array[Int]): Array[Int] = {
    //今日行程评分分布
    if (score <= 90) {
      scoreCountArray(0) = scoreCountArray(0) + 1
    } else if (score <= 92 && score >= 91) {
      scoreCountArray(1) = scoreCountArray(1) + 1
    } else if (score <= 94 && score >= 93) {
      scoreCountArray(2) = scoreCountArray(2) + 1
    } else if (score <= 97 && score >= 95) {
      scoreCountArray(3) = scoreCountArray(3) + 1
    } else {
      scoreCountArray(4) = scoreCountArray(4) + 1
    }
    scoreCountArray
  }

  /*def scoreCount(scoreArray: Array[Int], scoreCountArray: Array[Int]): Array[Int] = {
    //今日行程评分分布
    for (i <- scoreArray) {
      if (i < 60) {
        scoreCountArray(0) = scoreCountArray(0) + 1
      } else if (i < 70 && i >= 60) {
        scoreCountArray(1) = scoreCountArray(1) + 1
      } else if (i < 80 && i >= 70) {
        scoreCountArray(2) = scoreCountArray(2) + 1
      } else if (i <= 90 && i >= 80) {
        scoreCountArray(3) = scoreCountArray(3) + 1
      } else {
        scoreCountArray(4) = scoreCountArray(4) + 1
      }
    }
    scoreCountArray
  }*/

  def scorePercent(scoreCountArray: Array[Int], scorePercentArray: Array[String]): Array[String] = {
    //今日行程评分占比
    for (i <- 0 to 4) {
      var totalScores: Double = scoreCountArray.sum
      scorePercentArray(i) = (scoreCountArray(i) / totalScores).formatted("%.4f")
    }
    scorePercentArray
  }

  //加速度>3G的数量
  def totalThreeGTimes(totalThreeGTimes: Int, threeGTimes: Int): Int = {
    var threeGNew = totalThreeGTimes + threeGTimes
    threeGNew
  }
}
