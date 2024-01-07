from onlineObj import TradingObj,DcOnlineObj,writeReport,StockListOnlineObj,reportUpdateMark,ThsBonusOnlineObj,Dcjgyc
from stockMongodb import *
from concurrent.futures import ThreadPoolExecutor
from calMap import CalculMap


# 联网获取最新交易日期
def onlineLastDate():
	"""example 20220219"""
	op = TradingObj()
	onlineDate = op.isTrading()
	return onlineDate


# 获取本地数据库日期
def localLastDate():
	"""example 20220218"""
	db = StockMongo()
	dbLastDate = db.collectLastDate()
	lastDate = dbLastDate.replace("-","")
	return lastDate


# 判断是否需要更新股票列表
def stockListUpdate():
	"""20220218:更新;0:不更新"""
	onlineDate = onlineLastDate()
	localDate = localLastDate()

	if int(onlineDate) > int(localDate):
		print("Online:{},Database:{},Need Update!".format(onlineDate,localDate))
		return onlineDate
	else:
		print("Online:{},Database:{},It's already the latest data！".format(onlineDate,localDate))
		return 0


# 股票列表最新数据写入
def writeStockList(da):
	"""股票列表写入详情"""
	db = StockMongo()
	sk = StockListOnlineObj()
	sk.setLastDate(da)
	online = sk.dataForSql()
	# print(online)
	# return 

	if online:
		db = StockMongo()
		db.writeStockListData(online)
	else:
		pass


def updateEveryDay():
	"""每日更新"""
	mark = stockListUpdate()

	# print(mark)
	# return 
	if mark:
		writeStockList(mark)


def threadWriteReport(codeList):
	"""多线程写入数据(数据库没有数据)"""	

	with ThreadPoolExecutor(max_workers=90) as t:

		for i,j in t.map(writeReport,codeList):
			db = StockMongo()
			db.writeStock(CHECKSHEET,i)
			db.writeStock(STOCKREPORT,j)


def threadUpdateReport(codeList:list):
	"""多线程更新数据(数据库有数据)"""	
	if not codeList:
		return None

	for item in codeList:
		db = StockMongo()
		db.deleteStock(STOCKREPORT,{"SECURITY_CODE":item})
		db.deleteStock(CHECKSHEET,{"code":item})

	with ThreadPoolExecutor(max_workers=20) as t:

		for i,j in t.map(writeReport,codeList):
			db = StockMongo()
			db.writeStock(CHECKSHEET,i)
			db.writeStock(STOCKREPORT,j)


def needUpdateCodeList():
	"""需要更新的code数据"""
	db = StockMongo()
	code = db.newCodeInList()
	data = db.findNeedUpdateCode()
	code.extend(reportUpdateMark(data))
	return code


def reportUpdate():
	"""数据更新"""
	try:
		code = needUpdateCodeList()
		threadUpdateReport(code)
	except Exception as e:
		print(e)


# 分红对象
ths = ThsBonusOnlineObj("")


def getEachBonus(code:str):
	"""获取每个公司分红数据"""
	try:
		ths.setCode(code)
		return ths.getBonus()
	except Exception as e:
		print(e)
		return {}


def getAllBonus(codeList:list):
	"""批量获取分红数据并写入数据库"""

	# 获取数据库数据
	db = StockMongo()
	dbIter = db.findBonusSign() # 数据库数据{code,sign}

	# 数据库数据列表
	dbCodeList = []
	for i in dbIter:
		code = i.get("code")
		dbCodeList.append(code)
		bonusData = getEachBonus(code)
		onlineCount = len(bonusData.keys())

		if i.get("sign") != onlineCount: # 更新标志不一致则更新
			db.updateBonus(code,bonusData)

	# 不存在数据列表（新建数据）
	newCodeBonus = []
	for i in codeList:
		if i not in dbCodeList:
			eachBonus = getEachBonus(i)
			bonus = analyBonusData(i,eachBonus)
			if bonus:
				newCodeBonus.append(bonus)

	if newCodeBonus:
		db.writeBonus(newCodeBonus)


def analyBonusData(code:str,data:dict)->list:
	"""解析分红数据"""

	item = {
	"code":code,
	"field":"BONUS",
	"data":data,
	"sign":len(data.keys()),
	"creattime":time.strftime("%Y-%m-%d %H:%M:%S",time.localtime())
	}

	return item


def selfDataWrite(code:str,year:str,field:str,value):
	"""写入自行补录数据"""
	db = StockMongo()
	# 数据校验
	data = {
		"code":code,
		"year":year,
		"field":field,
		"value":float(value),
		"who":"admin",
		"idcard":0,
		"zan":99999,
		"createtime":time.strftime("%Y-%m-%d %H:%M:%S",time.localtime())
	}

	return db.insertSelfReport(data)


def selfDataDel(code:str,year:str,field:str):
	"""删除自行补录数据"""
	db = StockMongo()
	data = {
		"code":code,
		"year":year,
		"field":field
	}
	return db.delSelfReport(data)


def getSelfData(code:str):
	"""其他自定义数据明细"""
	db = StockMongo()
	sd = []

	# for fd in db.findOtherData(code):
	# 	for i ,j in fd["data"].items():
	# 		item = {}
	# 		item["year"] = i + "-12-31"
	# 		item["field"] = fd["field"]
	# 		item["method"] = "爬取"
	# 		item["value"] = round(j,2)
	# 		sd.append(item)

	for kv in db.findSelfData(code):
		item1 = {}
		item1["year"] = kv["year"]
		item1["field"] = kv["field"]
		item1["method"] = "手动"
		item1["value"] = round(kv["value"],2)
		item1["writetime"] = kv["createtime"]
		sd.append(item1)
	return sd


class DataPagBase:
	"""数据包"""
	def __init__(self,code,howLong=9):
		self.code = str(code)
		self.howLong = howLong + 1
		self.oth = self.otherData()
		self.slf = self.selfData()
		# self.rpt = self.reportData()


	def otherData(self):
		"""获取其他网络爬取数据"""
		db = StockMongo()
		data = {}
		for i in db.findOtherData(self.code):
			data[i["field"]] = i["data"]
		return data


	def selfData(self):
		"""获取自行补录数据"""
		db = StockMongo()
		data =  {}
		for i in db.findSelfData(self.code):
			year = i["year"].split("-")[0]
			if year in data:
				data[year].update({i["field"]:i["value"]})
			else:
				data[year] = {i["field"]:i["value"]}
		return data


	def onlineData(self):
		"""在线数据"""
		yc = Dcjgyc(self.code)
		ycdata = yc.pred()
		for i in range(len(ycdata)):
			if ycdata[i].get("EPS1") is not None and ycdata[i].get("EPS4") is not None and ycdata[i].get("EPS1") > 0 and ycdata[i].get("EPS4") > 0:
				ycdata[i]["fh3"] = str(round((pow(ycdata[i].get("EPS4",0) / ycdata[i].get("EPS1",0),1/3) - 1)*100,2)) + "%"
			ycdata[i]["EPS1"] = round(ycdata[i].get("EPS1",0) if ycdata[i].get("EPS1",0) else 0,2)
			ycdata[i]["EPS4"] = round(ycdata[i].get("EPS4",0) if ycdata[i].get("EPS4",0) else 0,2)
			# ycdata[i]["EPS2"] = round(ycdata[i].get("EPS2",0) if ycdata[i].get("EPS2",0) else 0,2)
			ycdata[i]["PE1"] = round(ycdata[i].get("PE1",0) if ycdata[i].get("PE1",0) else 0,2)
			ycdata[i]["PE4"] = round(ycdata[i].get("PE4",0) if ycdata[i].get("PE4",0) else 0,2)
		return ycdata


	def reportData(self):
		# pag [{"debt":{},"main":{},"benefit":{},"cash":{}},{},{}...]
		pag = []
		db = StockMongo()
		lastYear = time.localtime()[0] - 1
		for i in range(self.howLong):
			item = db.findCollectData(self.code,"{}-12-31".format(lastYear - i))
			if item:
				item["self"] = {}
				# 补充数据
				# 1、其他网络爬虫数据
				for k,v in self.oth.items():
					item["debt"].update({k:v.get(str(lastYear - i),0)}) 

				# 2、自行录入数据（包括错误修正,补充财务数据）
				if str(lastYear - i) in self.slf:
					item["self"].update(self.slf[str(lastYear - i)])
				pag.append(item)

		# 补入在线数据
		pag[0]["debt"]["jgyc"] = self.onlineData()
		pag[0]["debt"]["gujia"] = db.stockPrice(self.code)[0]
		return pag


	def dcfData(self):
		# pag [{"debt":{},"main":{},"benefit":{},"cash":{}},{},{}...]
		db = StockMongo()
		pag = []
		lastYear = time.localtime()[0] - 1
		for i in range(6):
			item = db.findCollectData(self.code,"{}-12-31".format(lastYear - i))
			if item:
				pag.append(item)

		# 补入在线数据
		if pag and pag[0].get("debt"):
			pag[0]["debt"]["gujia"],pag[0]["debt"]["gujiaDay"] = db.stockPrice(self.code)
		return pag


	def guzhi(self):
		dcf = self.dcfData()
		if dcf:
			ca = CalculMap(dcf)
			calcul = ca.calcul_layer()
			return calcul[:5]
		return []


	def calculate(self):
		rpt = self.reportData()

		if not rpt:
			print("Find Data Error!")
		
		ca = CalculMap(rpt)
		calcul = ca.calcul_layer()
		calcul.pop()

		return calcul


def getGuzhi(code):
	"""进行估值"""
	d = DataPagBase(code)
	if d:
		gzSet = d.guzhi()

	if not gzSet:
		print("%s ---------- 未知！" % (code))
	else:
		year_is_5 = len(gzSet)
		gzSet = gzSet[0]
		# print(gzSet)
	# return
	# if not gzSet[0]:
	# 	return "%s ---------- 未知！" % (code)
	# 估值参数输出
	# "公司:%s, 代码:%s, 年份:%s, ROE:%s, 毛利率:%s, 负债率:%s, 现金流:%s, 估值:%s, 股价(%s):%s, 收益率:%s"
	showStr = "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s" % (
			gzSet.get("SECURITY_NAME_ABBR"),
			code,
			year_is_5,
			gzSet.get("ROEAVG"),
			gzSet.get("MLLAVG"),
			gzSet.get("ZWAVG"),
			gzSet.get("XJL3_AVG"),
			gzSet.get("p7"),
			gzSet.get("gujiaDay"),
			gzSet.get("gujia"),
			gzSet.get("syl"))
	print(showStr)
	return showStr


def dcfAllStock():
	"""对所有股票进行估值"""
	from threading import Lock
	db = StockMongo()
	lock=Lock()
	cd = db.collectLastStock()
	# 过滤非主板股票
	main_code = ["60","00"]
	cd = [item for item in cd if item[:2] in main_code]
	# print(cd)
	# return 
	timeStr = time.strftime("%Y%m%d%H%M",time.localtime())
	n = 1 # 计数器
	an = len(cd) # 总数
	with open('value'+ timeStr +'.txt','a+') as f:
		f.write("公司, 代码, 年份, ROE, 毛利率, 负债率, 现金流, 估值, 股价日期, 股价, 收益率\n")
		with ThreadPoolExecutor(max_workers=35) as t:
			for gz in t.map(getGuzhi,cd):
				if gz:
					lock.acquire()
					# with open('value'+ timeStr +'.txt','a+') as f:
					f.write(gz+"\n")
					n += 1
					if n % 200 == 0:
						print("进度:%s / %s" % (n,an))
					lock.release()


if __name__ == '__main__':
	# 更新
	updateEveryDay()
	reportUpdate()
	# dcfAllStock()
	# bjs = ["831768"]
	# getGuzhi("600309")
	# threadUpdateReport(bjs)
	# getAllBonus(["600309"])
	# dl = db.checkCollectStock()
	# getAllBonus(dl)
	# selfDataWrite("002352","2020-12-31","BBLXZC",1036798173.98)
	# selfDataWrite("600309","2020-12-31","LXSR",326557053.69)
	# print(db.selfDataExist(da))
	# print(getSelfData("600309"))
	# dp = DataPagBase("002352")
	# data = dp.reportData()
	# cm = CalculMap(data)
	# cm.calcul_layer()

  

