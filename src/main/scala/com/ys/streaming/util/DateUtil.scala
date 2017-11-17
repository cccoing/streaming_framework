package com.ys.streaming.util

import java.text.SimpleDateFormat
import java.util.Date

/**
  * Created by yangshuo on 2017/11/17.
  */
object DateUtil {

  def getNowDate():String={
    var now:Date = new Date()
    var  dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    dateFormat.format( now )
  }

}
