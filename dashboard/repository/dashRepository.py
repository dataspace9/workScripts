from datetime import date, datetime, timedelta

from flask import current_app

from collections import defaultdict, Counter
from controllers.cryptController import cryptography
import locale
import time
class DashRepository:
    def __init__(self, db, mdb,dash_db,mysql_customer_db):
        locale.setlocale(locale.LC_MONETARY, 'en_IN')
        self.client_db = db
        self.master_db = mdb
        self.dash_db = dash_db
        self.mysql_customer_db = mysql_customer_db

    def get_all_lead_details(self):
        tic = time.perf_counter()
        mycursor = self.mysql_customer_db.cursor()
        mycursor.execute("SELECT created_at FROM customers")
        all_lead_details = []
        for item in mycursor.fetchall():
            new_item = [*item,]
            all_lead_details.append(new_item[0].strftime("%Y-%m-%d"))
        toc = time.perf_counter()
        print(f" getLeadDetails took {toc-tic} seconds")
        return all_lead_details

    def cancelled_plan_count(self,code):
        return len([x["proposalId"] for x in self.client_db["protectionplans"].find({ "status": {"$in":["Expired","Cancelled"]},"partnerName":code}, {"_id": 0, "proposalId": 1})])


    def getCreditScoresData(self):
        creditScoreData = list(
            self.client_db["quotes"].aggregate(
                [
                    {"$group": {"_id": {"creditScore": "$request.creditScore", "mobile": "$request.mobile"}}},
                    {"$project": {"mobile": "$_id.mobile", "creditScore": "$_id.creditScore", "_id": 0}},
                ]
            )
        )

        return creditScoreData

    def getPlanAmountCount(self, x):
        return self.client_db["quotes"].count_documents({"response.plans.coverages.protectionAmount": x})

    def getPlanUniqueAmountCount(self, x):
        planCount = [x['request']['mobile'] for x in self.client_db["quotes"].find({"response.plans.coverages.protectionAmount": x},{"_id":0,"request.mobile":1})]
        return len(list(set(planCount)))

    def add_company_details(self,plan_details):
        quote_details =  list(self.master_db["assurekitcompanymasters"].aggregate(
                [
                    {"$match": {"companyId": {"$in": [x['companyId'] for x in plan_details]}}},
                    {
                        "$project": {
                            "_id": 0,
                            "name": "$name",
                            "companyId":"$companyId" ,  
                            "companyPincode":"$companyPincode"   
                        }
                    },
                ]
            ))
        [x.update({"companyName":y['name'],"companyPincode":int(y['companyPincode']+"1"),"companyPincodeId":y['companyId']}) for x in plan_details for y in quote_details if x['companyId'] == y['companyId']] 
        return plan_details
    
    def get_pincode_masters(self):
        state_city_details =  list(self.master_db["assurekitpincodemasters"].aggregate(
                [
                    {
                        "$project": {
                            "_id": 0,
                            "companyPincodeId": "$id",
                            "state":1 ,  
                            "stateCode":1,
                            "city":1   
                        }
                    },
                ]
            ))
        return state_city_details
    
    def get_quotes(self):
        quote_details =  list(self.client_db["quotes"].aggregate(
                [
                    {
                        "$project": {
                            "_id": 0,
                            "companyPincodeId":"$request.employment.companyPincodeId",
                            "mobile":"$request.mobile"
                        }
                    },
                ]
            ))
        return quote_details
    
    def add_company__state_details(self,plan_details):
        companystate =  list(self.master_db["assurekitpincodemasters"].aggregate(
                [
                    {"$match": {"pincode": {"$in": [x['companyPincode'] for x in plan_details]}}},
                    {
                        "$project": {
                            "_id": 0,
                            "stateCode":1 ,  
                            "pincode":1,
                            "city":1  
                        }
                    },
                ]
            ))
        [x.update({"companystate":y['stateCode'],"city":y["city"]}) for x in plan_details for y in companystate if x['companyPincode'] == y['pincode']] 
        return plan_details
    
    def get_state_name(self,state_code):
        return self.master_db["assurekitpincodemasters"].find_one({"stateCode":state_code},{"_id":0,"state":1})

    def get_client_details(self):
        return [x for x in self.client_db["clients"].find({},{"_id":0,"clientId":1,"fullName":1,"code":1})]

    def plan_details_with_proposal_quote(self):
        planComponents = list(self.client_db['protectionplans'].aggregate([
                {
                    '$match': {"status":"Active"
                            #    ,"planStartDate":{}
                            #    {"$gte":1688149800}
                    }
                }, {
                    '$lookup': {
                        'from': 'proposals', 
                        'localField': 'proposalId', 
                        'foreignField': 'response.proposalId', 
                        'as': 'proposal'
                    }
                }, {
                    '$lookup': {
                        'from': 'quotes', 
                        'localField': 'proposal.request.planId', 
                        'foreignField': 'response.plans.id', 
                        'as': 'quote'
                    }
                },
                {
                "$project": {
                    "status":1,
                    "createdAt":1,
                    "proposalId":1,
                    "partnerName":1,
                    "clientId":{
                        '$arrayElemAt': [
                            '$quote.request.channelPartner', 0
                        ]
                    },
                    "year": {"$year": "$createdAt"},
                    "month": {"$month": "$createdAt"},
                    # "date": {"$dateToString": {"format": "%Y-%m-%d-%H:%M:%S:%L%z","timezone":"+05:30","date": "$createdAt"}},
                    "date":{"$add":["$createdAt",5.5*60*60*1000]},

                    "_id":0,
                'quoteId': {
                    '$arrayElemAt': [
                        '$proposal.request.planId', 0
                    ]
                }, 
                "netAmount":{
                    '$arrayElemAt': [
                        '$proposal.response.premium.netAmount', 0
                    ]
                },
                "policyAmount":{
                    '$arrayElemAt': [
                        '$proposal.response.premium.protectionAmount', 0
                    ]
                }, 
                "pinId":{
                    '$arrayElemAt': [
                        '$quote.request.employment.companyPincodeId', 0
                    ]
                },
                "creditScore":{
                    '$arrayElemAt': [
                        '$quote.request.creditScore', 0
                    ]
                },
                "salary":{
                    '$arrayElemAt': [
                        '$quote.request.salary', 0
                    ]
                },
                "pan":{
                    '$arrayElemAt': [
                        '$proposal.request.proposer.pan', 0
                    ]
                },
                "pincode":{
                    '$arrayElemAt': [
                        '$proposal.request.communication.pincode', 0
                    ]
                },
                "state":{
                    '$arrayElemAt': [
                        '$proposal.request.communication.state', 0
                    ]
                },
                "gender":{
                    '$arrayElemAt': [
                        '$proposal.request.proposer.gender', 0
                    ]
                },
                "customerName":{
                    '$arrayElemAt': [
                        '$proposal.request.proposer.name', 0
                    ]
                },
                "companyId":{
                    '$arrayElemAt': [
                        '$proposal.request.employment.companyId', 0
                    ]
                },
                'plans': {
                    '$arrayElemAt': [
                        '$quote.response.plans', 0
                    ]
                }, 
                }
            }
        ]))
        for i in planComponents:
            if i.get('plans'):
                t = [x for x in i['plans'] if x['id'] == i['quoteId']]
                if t:
                    i['salaryCover'] = [x['protectionAmount'] for x in t[0]['coverages'] if x['code'] == 'IPSAL'][0]
                    # i['expenseCover'] = [x['protectionAmount'] for x in t[0]['coverages'] if x['code'] == 'IPAKB'][0]
                del i['plans']
            else:
                i['salaryCover'] = 0 
                i['expenseCover'] = 0 
            i['premiumWithINR'] = locale.currency(round(float(i['netAmount']),2),grouping=True)
            i['salaryCoverWithINR'] = locale.currency(round(float(i['salaryCover']),2),grouping=True)
            # i['expenseCoverWithINR'] = locale.currency(round(float(i['expenseCover']),2),grouping=True)
        return planComponents


    def proposal_lookup_partner(self):
        partnerlevel = [x for x in self.client_db["proposals"].aggregate([
            {
                    '$lookup': {
                        'from': 'quotes', 
                        'localField': 'request.planId', 
                        'foreignField': 'response.plans.id', 
                        'as': 'quote'
                    }
                },
                {
                "$project": {
                    "_id":0,
                "channelPartner":{
                    '$arrayElemAt': [
                        '$quote.request.channelPartner', 0
                    ],
                },
                "mobile":{
                    '$arrayElemAt': [
                        '$quote.request.mobile', 0
                    ]
                },
                }
                }
        ])]
        return partnerlevel

    def payment_lookup_partner(self):
        partnerlevel = [x for x in self.client_db["payments"].aggregate([
            {
                    '$lookup': {
                        'from': 'proposals', 
                        'localField': 'proposalId', 
                        'foreignField': 'id', 
                        'as': 'proposal'
                    }
                },
                {
                    '$lookup': {
                        'from': 'quotes', 
                        'localField': 'proposal.request.planId', 
                        'foreignField': 'response.plans.id', 
                        'as': 'quote'
                    }
                },
                {
                "$project": {
                    "_id":0,
                "channelPartner":{
                    '$arrayElemAt': [
                        '$quote.request.channelPartner', 0
                    ]
                },
                 "mobile":{
                    '$arrayElemAt': [
                        '$quote.request.mobile', 0
                    ]
                }
                }
                }
        ])]
        return partnerlevel
    
    def payment_count(self):
        return self.client_db["payments"].count_documents({})

    def quote_count(self,code):
        return [x['mobile'] for x in self.client_db["quotes"].find({"request.channelPartner":code},{"_id":0,"channelPartner":"$request.channelPartner","mobile":"$request.mobile"})]
 
    
    def partner_quote(self,code):
        return [x for x in self.client_db["quotes"].find({"request.channelPartner":code},{"_id":0,"response":1,"request":1})] 


    def declined_count(self):
        return self.client_db["quotes"].count_documents({"response.plans":[]}) 

    def declined_count_on_date(self,date,current_date_data):
        leadCountsByDate = list(self.client_db["quotes"].aggregate([
        {
            '$addFields': {
                "created_at_new": {"$dateToString": {"format": "%Y-%m-%d", "date": "$createdAt"}},
            }
        },
        {
            '$match': {
                'response.plans': [],
                'created_at_new':{"$lte":date} if not current_date_data else date
            }
        },
        {
            '$project': {
                "created_at_new": "$created_at_new",
            }
        },
    ]))
        return len(leadCountsByDate)

    def lead_created_count(self):
        return self.client_db["auditlogs"].count_documents({}) + self.dash_db["incomeProtectLeads"].count_documents({})
    

    