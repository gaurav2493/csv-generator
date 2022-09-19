

import java.util.*

class UserTransaction {
  var userId: String? = null
  var inf: String? = null
  var orderId: String? = null
  var timeStamp: Date? = null
  var amount: Double? = null
  var price: Double? = null
  var unitsAllotted: Double? = null
  var primarySource: String? = null
  var folio: String? = null
  var medium: Int? = null
  var status: Int? = null

  fun toCsvRow(): String {
    return "$userId,$inf,$amount,$folio,$price$timeStamp,$unitsAllotted,$status\n"
  }
}