package logic

import utils.CtlUtils

class CtlStatistics(val ctlUtils: CtlUtils) {
	def publishCtlStatistics(resultCount: Long, loading_id: String, entity_id: String):Unit = {
		val statResult = if (resultCount > 0) "4" else "1"
		ctlUtils.createStatistic(loading_id.toInt, entity_id, 32, statResult)
	}
}
