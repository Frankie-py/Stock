import pymongo
import datetime
import time
from stockSetting import *
# Windows 数据库用户密码
# amdin admin
# root 951224
# stock 951224

class MgClient(object):
    '''通过重载实例化函数__new__缓存mongodb连接'''
    conn=None
    def __new__(cls,*args,**kwds):
        if cls.conn is None:
            cls.conn=pymongo.MongoClient(
			host="localhost",
			port=27017,
			authSource="Stock")
        return cls.conn


class StockMongo:
	"""Mongo数据库操作对象"""
	def __init__(self):
		self.client = MgClient()
		self.db = self.client["Stock"]
		self.session = self.client.start_session()


	def checkCollectIsNull(self,collect):
		"""检查集合是否为空"""
		if self.db[collect].count_documents({}) == 0:
			return 1
		else:
			return 0


	def writeStock(self,collect,stockData):
		"""写入数据"""
		self.db[collect].insert_many(stockData)
		return 1


	def writeStockListData(self,stockData):
		"""写入股票列表数据"""
		self.session.start_transaction()

		try:
			self.db[STOCKLIST].insert_many(stockData)
		except:
			self.session.abort_transaction()
		else:
			self.session.commit_transaction()
		finally:
			self.session.end_session()


	def readStock(self,collect,mgsql):# mgsql:{"date":{"$gt":datetime.datetime(2021,12,27)}},{"_id":0,"code":1,"name":1,"date":1}
		"""读取数据"""
		data = []
		for i in self.db[collect].find(mgsql):
			data.append(i)
		return data


	def deleteStock(self,collect,mgsql): #mgsql:{"date":{"$gt":datetime.datetime(2021,12,27)}}
		"""删除符合条件的数据"""

		try:
			self.db[collect].delete_many(mgsql)
		except Exception as e:
			print(e)


	def collectLastDate(self):
		"""获取数据库记录最大日期"""
		for i in self.db[STOCKLIST].find({},{"_id":0,"date":1}).sort("date",-1).limit(1):
			# print(i)
			collLastDate = i.get("date")
			return collLastDate


	def collectLastStock(self):
		"""股票列表数据库中最新的股票列表"""
		stock = []
		lastDate = self.collectLastDate()
		for i in self.db[STOCKLIST].find({"date":lastDate},{"_id":0,"code":1}):
			stock.append(i.get("code"))

		stockSet = set(stock)
		stockList = list(stockSet)
		return stockList


	def stockPrice(self,code):
		"""股票列表数据库中最新的股票价格"""
		lastDate = self.collectLastDate()

		for i in self.db[STOCKLIST].find({"date":lastDate,"code":code},{"_id":0,"code":1,"price":1}):
			return i.get("price",0),lastDate[-6:]

		return 0,lastDate[-6:]


	def checkCollectStock(self):
		"""检查数据表的股票列表"""
		stock = []
		for i in self.db[CHECKSHEET].find({"report":"main","reportType":"bgq"},{"_id":0,"code":1}):
			stock.append(i.get("code"))
			
		stockSet = set(stock)
		stockList = list(stockSet)
		return stockList


	def newCodeInList(self):
		"""获取新增数据"""
		codeL = []
		n = self.collectLastStock()
		o = self.checkCollectStock()

		for i in n:
			if i not in o:
				codeL.append(i)

		return codeL


	def checkIpPool(self):
		"""检查IP池可用数据"""
		ippool = []
		for i in self.db["stockIp"].find({"isUse":1},{"_id":0,"ip":1}):
			ippool.append(i.get("ip"))

		ipSet = set(ippool)
		ipList = list(ipSet)
		return ipList


	def updateIpPool(self,ipStr):
		"""更新IP数据库可用数据"""
		if isinstance(ipStr,str):
			self.db["stockIp"].update_many({"ip":ipStr},{"$set":{'isUse':0}})


	def reportLastStock(self):
		"""财务数据库中最新的股票列表"""
		stock = []
		for i in self.db[STOCKREPORT].find({"reportType":"debt"},{"_id":0,"SECURITY_CODE":1}):
			stock.append(i.get("SECURITY_CODE"))

		stockSet = set(stock)
		stockList = list(stockSet)
		return stockList


	def checkCollectIsNull(self,collect):
		"""检查集合是否为空"""
		if self.db[collect].count_documents({}) == 0:
			return 1
		else:
			return 0


	def addIndex(self):
		"""添加索引"""
		self.db[STOCKREPORT].create_index([('SECURITY_CODE',1)],name = "code_index")
		self.db[CHECKSHEET].create_index([('code',1)],name = "code_index")


	def dropIndex(self):
		"""删除索引"""
		self.db[STOCKREPORT].drop_index("code_index")
		self.db[CHECKSHEET].drop_index("code_index")


	def findCollectData(self,code,cyears):
		"""查找财务数据"""
		# code:600309
		# report:main
		# reportType:bgq
		# cyears:2021
		item = {}

		for i in self.db[STOCKREPORT].find({"SECURITY_CODE":code,"reportType":{"$ne":"djd"},"REPORT_DATE":"{} 00:00:00".format(cyears)},{"_id":0}):
			# print(i)
			item[i.get("report")] = i

		return item


	def findNeedUpdateCode(self):
		"""查找需要更新的数据"""
		codeList = []
		for i in self.db[CHECKSHEET].find({"report":"main","reportType":"bgq"},{"_id":0,"dateUrl":1,"count":1,"code":1}):
			codeList.append(i)

		return codeList


	def insertSelfReport(self,data)->int:
		"""插入自定义数据""" # code,years,report
		if isinstance(data,dict):
			# 自定义数据是否已经存在
			if self.selfDataExist(data):
				# 更新
				self.selfUpdateReport(data)
				return 2
			else:
				self.db[SELFREPORT].insert_one(data)
				return 1
		else:
			print("Input Data Type Error！")
			return 0 


	def delSelfReport(self,data)->int:
		"""删除自定义数据""" # code,year,fid
		if isinstance(data,dict):
			code = data.get("code","000000")
			field = data.get("field","000000")
			year = data.get("year","000000")
			self.db[SELFREPORT].delete_many({"code":code,"field":field,"year":year})
			return 1
		else:
			print("Input Data Type Error！")
			return 0 


	def selfDataExist(self,data):
		"""检验数据是否已经存在"""
		dataCount = self.db[SELFREPORT].count_documents({"code":data["code"],"year":data["year"],"field":data["field"],"idcard":0})
		return dataCount


	def selfUpdateReport(self,data):
		"""更新自定义数据"""
		self.db[SELFREPORT].update_one({"code":data["code"],"year":data["year"],"field":data["field"],"idcard":0},{"$set":{'value':data["value"]}})


	def findSelfData(self,code):
		"""查询自行补录数据"""
		selfIter = self.db[SELFREPORT].find({"code":code,"idcard":0},{"_id":0,"year":1,"field":1,"value":1,"createtime":1}).sort("createtime",-1)
		
		if selfIter:
			return selfIter

		return []	


	def findBonusSign(self):
		"""查询网络爬取分红数据用于更新"""
		bonusIter = self.db[OTHERDATA].find({"field":"BONUS"},{"_id":0,"code":1,"sign":1})
		if bonusIter:
			return bonusIter
		return []


	def findOtherData(self,code):
		"""查找其他网络爬取数据"""
		Iter = self.db[OTHERDATA].find({"code":code},{"_id":0,"code":1,"field":1,"data":1})
		if Iter:
			return Iter

		return []


	def writeBonus(self,data)->int:
		"""写入分红数据"""
		if isinstance(data,dict):
			self.db[OTHERDATA].insert_one(data)
			return 1
		elif isinstance(data,list):
			self.db[OTHERDATA].insert_many(data)
			return 2
		else:
			print("Input Data Type Error！")
			return 0 	


	def updateBonus(self,code:str,data:dict):
		"""更新分红数据""" 
		# data:{"code":600309,"field":"BONUS",
		# 	"data":{"2022":5798235,"2021":137913},"sign":22,"createtime":"2022-05-03"}
		# 查找删除
		self.session.start_transaction()

		try:
			self.db[OTHERDATA].delete_many({"code":code,"field":"BONUS"})
			self.writeBonus(code,data)
		except:
			self.session.abort_transaction()
		else:
			self.session.commit_transaction()



# 全局数据库对象
# db = StockMongo()


if __name__ == '__main__':
	# db = StockMongo()
	# print(db.findCollectData("600309","2021-12-31"))
	# print(db.newCodeInList())
	# db.dropIndex()
	# db.addIndex()
	# db.findCollectData("600309","cash","2021-06-30")
	# print(db.findNeedUpdateCode())
	# db.addIndex()
	# from onlineObj import StockListOnlineObj
	# sk = StockListOnlineObj()
	# online = sk.dataForSql()
	# s = StockMongo()
	# print(s.checkIpPool())
	# s.writeStock("test",online)
	# reportStock = s.reportLastStock()
	# reportStock = s.collectLastStock()
	# print(reportStock)
	# print(len(reportStock))
	# print(s.checkCollectIsNull("report"))
	# print(s.collectLastStock())
	# print(db.findSelfBonus("600309"))
	# print(db.checkCreateSelf("600309"))
	# db.updateBonus("600309",{"2022":784936.66,"2021":408167.06})
	# print(db.stockPrice("600309"))
	pass
