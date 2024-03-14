import string,locale
from datetime import date, datetime, timedelta
from dateutil.tz import tzlocal 
import time
import numpy as np
import pandas as pd
import plotly.express as px
from dash import dash_table, dcc, html

from repository.dashRepository import DashRepository


class DashDetailsController:
    def __init__(self, db, mdb,dash_db,mysql_customer_db) -> None:
        tic = time.perf_counter()
        locale.setlocale(locale.LC_MONETARY, 'en_IN')
        self.policyColumns = ["Name","Premium","ProtectionAmount","date"]
        self.repo = DashRepository(db, mdb,dash_db,mysql_customer_db)
        self.date = date.today()
        self.headerStyle = {"backgroundColor": "rgb(210, 210, 210)", "color": "black", "fontWeight": "bold"}
        # self.plansData = {
        #     "salaryComponent": [30000, 75000, 150000, 210000, 300000, 450000, 600000],
        #     "assurekitBenefits": [10000, 25000, 50000, 50000, 50000, 50000, 50000],
        #     "Total": [40000, 100000, 200000, 260000, 350000, 500000, 650000],
        # }
        self.dataStyle = {"color": "black", "backgroundColor": "white"}

        self.dataCondition = [
            {
                "if": {"row_index": "odd"},
                "backgroundColor": "rgb(220, 220, 220)",
            }
        ]
        self.style = {"textAlign": "center"}
        self.client_list =  self.repo.get_client_details()
        plan_details = self.repo.plan_details_with_proposal_quote()
        self.lead_details = self.repo.get_all_lead_details()
        self.datewise_lead_data,self.datewise_different_lead_data,self.datewise_different_percentage_data = self.getDailyLeadCount()
        plan_details = self.repo.add_company_details(plan_details)
        self.plan_details = self.repo.add_company__state_details(plan_details)

        self.overall_summary_table_data = []
        self.overall_summary_table_unique_data = []
        proposal_lookup = self.repo.proposal_lookup_partner()
        payment_lookup = self.repo.payment_lookup_partner()
        for i in self.client_list:
            quote = self.repo.quote_count(i.get('clientId'))
            proposal_count = [x.get('mobile',"") for x in proposal_lookup if  x.get('channelPartner','') == i.get('clientId')]
            payment_count = [x.get('mobile',"") for x in payment_lookup if  x.get('channelPartner','') == i.get('clientId')]
            unique_proposal_count = list(set(proposal_count))
            unique_payment_count = list(set(payment_count))
            unique_quote = list(set(quote))
            plan_by_client = len([x for x in self.plan_details if 'clientId' in x and  x['clientId'] == i.get('clientId')])
            strikeRate = 0
            unique_strikeRate = 0
            if plan_by_client and quote:
                strikeRate = round(float(plan_by_client/len(quote)) * 100,2)
                unique_strikeRate = round(float(plan_by_client/len(unique_quote)) * 100,2)
            dict = {
                "Partner":i.get('code'),
                "Lead Count":"NA",
                "Quote count": len(quote),
                "Proposals Count": len(proposal_count),
                "Payments Count": len(payment_count),
                "Plan Converted Count": len([x for x in self.plan_details if 'clientId' in x and x['clientId'] == i.get('clientId')]),
                "Strike Rate": strikeRate, 
            }
            unique_count_dict = {
                "Partner":i.get('code'),
                "Lead Count":"NA",
                "Quote count": len(unique_quote),
                "Proposals Count": len(unique_proposal_count),
                "Payments Count": len(unique_payment_count),
                "Plan Converted Count": len([x for x in self.plan_details if 'clientId' in x and x['clientId'] == i.get('clientId')]),
                "Strike Rate": unique_strikeRate, 
            }
            self.overall_summary_table_data.append(dict)
            self.overall_summary_table_unique_data.append(unique_count_dict)

        self.credit_score_data = self.repo.getCreditScoresData()
        self.allPolicyDetailsData = []

        quote_details = self.repo.get_quotes()
        state_city_details = self.repo.get_pincode_masters()
        quote_data_frame = pd.DataFrame(quote_details)
        # print(quote_data_frame)
        state_city_data_frame = pd.DataFrame(state_city_details)
        # print(state_city_data_frame)
        quote_data_frame.merge(state_city_data_frame, on='companyPincodeId', how='left')
        # print()
        self.quote_merged_with_state_city=pd.merge(quote_data_frame,state_city_data_frame, on='companyPincodeId', how='left')
        # print(self.quote_merged_with_state_city)

        # print(self.quote_merged_with_state_city)
        toc = time.perf_counter()
        print(f"DashDetailsController init executed in {toc - tic:0.4f} seconds")
    
    def getDailyLeadCount(self):
        unique_date = list(set(self.lead_details))
        if unique_date:
            unique_date.sort(key=lambda x: datetime.strptime(x, "%Y-%m-%d"),reverse=True)
            from collections import Counter
            temp = Counter(self.lead_details)
            datewise_lead_data = []
            datewise_different_lead_data = []
            datewise_different_percentage_data = []
            for date in unique_date[:10]:
                total_lead_count_until_today = len([x for x in self.lead_details if datetime.strptime(x,"%Y-%m-%d") <= datetime.strptime(date,"%Y-%m-%d")])
                lead_count_percentage = round((temp[date]/(total_lead_count_until_today-temp[date]))*100,2)
                not_eleigble_quotes_til_today = self.repo.declined_count_on_date(date,False)
                quotes_declined_by_today = self.repo.declined_count_on_date(date,True)
                elegible_percentage = round((quotes_declined_by_today/(not_eleigble_quotes_til_today-quotes_declined_by_today))*100,2)
                datewise_lead_data.append(dict(Date = date, leadCount = total_lead_count_until_today,notEligible = not_eleigble_quotes_til_today,noExperion= "NA"))
                datewise_different_lead_data.append(dict(Date = date, leadCount = temp[date],notEligible = quotes_declined_by_today,noExperion= "NA"))
                datewise_different_percentage_data.append(dict(Date = date, leadCount = lead_count_percentage,notEligible = elegible_percentage,noExperion= "NA"))
            return datewise_lead_data,datewise_different_lead_data,datewise_different_percentage_data
        else:
            return [],[],[]
        
    def getDailyLeadChart(self):
        tic = time.perf_counter()
        lead_date_wise_summary = pd.DataFrame(self.datewise_lead_data)
        leadSummaryTable = dash_table.DataTable(
        self.datewise_lead_data,
            [{"name": i, "id": i} for i in lead_date_wise_summary.columns],
            page_size=10,
            style_cell={"textAlign": "left"},
            style_header=self.headerStyle,
            style_data=self.dataStyle,
            style_data_conditional=self.dataCondition,
        )
        toc = time.perf_counter()
        print(f"DashDetailsController getDailyLeadChart executed in {toc - tic:0.4f} seconds")
        return leadSummaryTable
    
    def getDailyLeadDifferentChart(self):
        tic = time.perf_counter()
        lead_date_wise_summary_daily = pd.DataFrame(self.datewise_different_lead_data)
        leadSummaryDailyTable = dash_table.DataTable(
        self.datewise_different_lead_data,
            [{"name": i, "id": i} for i in lead_date_wise_summary_daily.columns],
            page_size=10,
            style_cell={"textAlign": "left"},
            style_header=self.headerStyle,
            style_data=self.dataStyle,
            style_data_conditional=self.dataCondition,
        )
        toc = time.perf_counter()
        print(f"DashDetailsController getDailyLeadDifferentChart executed in {toc - tic:0.4f} seconds")
        return leadSummaryDailyTable
    
    def getDailyLeadPercentageChart(self):
        tic = time.perf_counter()
        lead_date_wise_summary_daily_percentage = pd.DataFrame(self.datewise_different_percentage_data)
        leadSummaryDailyPercentageTable = dash_table.DataTable(
        self.datewise_different_percentage_data,
            [{"name": i, "id": i} for i in lead_date_wise_summary_daily_percentage.columns],
            page_size=10,
            style_cell={"textAlign": "left"},
            style_header=self.headerStyle,
            style_data=self.dataStyle,
            style_data_conditional=self.dataCondition,
        )
        toc = time.perf_counter()
        print(f"DashDetailsController getDailyLeadChart executed in {toc - tic:0.4f} seconds")
        return leadSummaryDailyPercentageTable

    # def format(x):
    #     return "₹{:,}".format(x)

    # def removeDecimals(x):
    #     return round(x, 2)

    def plans_created_today(self):
        tic = time.perf_counter()
        active_policy = [x for x in self.plan_details if x['status'] == "Active" and x['date'].date() == self.date]
        policySummaryData = [
            {
                "Total Plans": len(active_policy),
                "Total Plan Cost": locale.currency(round(sum(float(x['netAmount']) for x in active_policy),2), grouping=True),
                "Total  Salary Cover":locale.currency(round(sum(float(x['salaryCover']) for x in active_policy),2), grouping=True),
                # "Total Expense Cover": locale.currency(round(sum(float(x['expenseCover']) for x in active_policy),2), grouping=True),
            }
        ]
        policySummary = pd.DataFrame(policySummaryData)
        policySummaryTable = dash_table.DataTable(
           policySummaryData,
            [{"name": i, "id": i} for i in policySummary.columns],
            page_size=10,
            style_cell={"textAlign": "left"},
            style_header=self.headerStyle,
            style_data=self.dataStyle,
            style_data_conditional=self.dataCondition,
        )
        toc = time.perf_counter()
        print(f"DashDetailsController plans_created_today executed in {toc - tic:0.4f} seconds")
        return policySummaryTable
    

    def plans_created_till_now(self):
        tic = time.perf_counter()
        active_policy = [x for x in self.plan_details if x['status'] == "Active"]
        policySummaryData= []
        for i in self.client_list:
            dict = {
                    "Partner": i.get('code'),
                    "Active Plans": len([x for x in active_policy if x.get('partnerName') == i.get('code')]),
                    "TotalPlancost": round(sum(float(x['netAmount']) for x in active_policy if x.get('partnerName') == i.get('code')),2),
                    "Totalsalarycover":round(sum(float(x['salaryCover']) for x in active_policy if x.get('partnerName') == i.get('code')),2),
                    # "Totalexpensecover": round(sum(float(x['expenseCover']) for x in active_policy if x.get('partnerName') == i.get('code')),2),
                }
            dict["Total Plan Cost"]= locale.currency(dict['TotalPlancost'], grouping=True)
            dict["Total Salary Cover"]= locale.currency(dict['Totalsalarycover'], grouping=True)
            # dict["Total Expense Cover"]= locale.currency(dict['Totalexpensecover'], grouping=True)
            
            policySummaryData.append(dict)
        policySummaryData.sort(key=lambda x: x["TotalPlancost"],reverse=True)
        policySummaryData.append({"Partner":"Total","Active Plans":round(sum(float(x['Active Plans']) for x in policySummaryData),2), "Total Plan Cost":locale.currency(round(sum(float(x['TotalPlancost']) for x in policySummaryData),2), grouping=True),
                                  "Total Salary Cover":locale.currency(round(sum(float(x['Totalsalarycover']) for x in policySummaryData),2), grouping=True)})
                                #   "Total Expense Cover":locale.currency(round(sum(float(x['Totalexpensecover']) for x in policySummaryData),2), grouping=True)})
        policySummary = pd.DataFrame(policySummaryData)
        # policySummary = policySummary[["Partner","Active Plans","Total Plan Cost","Total Salary Cover","Total Expense Cover"]]
        policySummary = policySummary[["Partner","Active Plans","Total Plan Cost","Total Salary Cover"]]

        policySummaryTable = dash_table.DataTable(
           policySummaryData,
            [{"name": i, "id": i} for i in policySummary.columns],
            page_size=10,
            style_cell={"textAlign": "left"},
            style_header=self.headerStyle,
            style_data=self.dataStyle,
            style_data_conditional=self.dataCondition,
        )
        toc = time.perf_counter()
        print(f"DashDetailsController plans_created_till_now executed in {toc - tic:0.4f} seconds")
        return policySummaryTable
    
    def getAllCreditScoresDataTable(self):
        tic = time.perf_counter()
        creditScoreData = [x["creditScore"] for x in self.credit_score_data]
        bins = [x for x in range(min(creditScoreData), max(creditScoreData)+50, 50)]
        creditScoreData = pd.DataFrame(creditScoreData)
        labels = list(string.ascii_uppercase)

        creditScoreData.rename(columns={0: "Credit Scores"}, inplace=True)
        creditScoreData["category"] = pd.cut(creditScoreData["Credit Scores"], bins, labels)
        creditBuckets = creditScoreData.groupby("category").count().reset_index()
        creditBuckets = pd.DataFrame(creditBuckets[creditBuckets["Credit Scores"] != 0])
        creditBuckets["category"] = creditBuckets["category"].astype(str)
        creditBuckets.rename(columns={"category": "Credit score band"}, inplace=True)
        creditBuckets.rename(columns={"Credit Scores": "Quote count"}, inplace=True)
        allCreditScoresDataTable = dash_table.DataTable(
            creditBuckets.to_dict("records"),
            [{"name": i, "id": i} for i in creditBuckets.columns],
            style_cell={"textAlign": "left"},
            style_header=self.headerStyle,
            style_data=self.dataStyle,
            style_data_conditional=self.dataCondition,
        )
        toc = time.perf_counter()
        print(f"DashDetailsController getAllCreditScoresDataTable executed in {toc - tic:0.4f} seconds")
        # page_size=10)
        return allCreditScoresDataTable

    def getCreditScoresGraph(self):
        tic = time.perf_counter()
        creditScoreData = [x["creditScore"] for x in self.credit_score_data]
        creditScoreData = pd.DataFrame(creditScoreData)
        creditScoreData.rename(columns={0: "Credit Scores"}, inplace=True)
        fig = px.histogram(creditScoreData, x="Credit Scores", nbins=80)
        creditScoresChart = html.Div([dcc.Graph(figure=fig)])
        toc = time.perf_counter()
        print(f"DashDetailsController getCreditScoresGraph executed in {toc - tic:0.4f} seconds")
        return creditScoresChart

    def getCharts(self):
        if self.plan_details == []:
            charts = html.Div([])
        else:
            tic = time.perf_counter()
            allpolicyData = pd.DataFrame(self.plan_details)
            allpolicyData['date'] = allpolicyData['date'].dt.date 
            dataForPlots = allpolicyData.groupby("date").agg({"proposalId": "count", "netAmount": "sum"}).reset_index()
            dataForPlots.rename(columns={"proposalId": "Policy count"}, inplace=True)
            dataForPlots.rename(columns={"netAmount": "Net Amount"}, inplace=True)
            dataForPlots.rename(columns={"date": "Date"}, inplace=True)

            policyCountTimeFig = px.bar(dataForPlots, x=dataForPlots["Date"], y=dataForPlots["Net Amount"])
            policyCountPremiumFig = px.bar(dataForPlots, x=dataForPlots["Date"], y=dataForPlots["Policy count"])
            charts = html.Div([dcc.Graph(figure=policyCountTimeFig), dcc.Graph(figure=policyCountPremiumFig)])
            toc = time.perf_counter()
            print(f"DashDetailsController getCharts executed in {toc - tic:0.4f} seconds")
        return charts

    def getAllPoliciesDataTable(self):
        if self.plan_details == []:
            return html.H6("*******No policies punched till date********")
        else:
            tic = time.perf_counter()
            allpolicyData = pd.DataFrame(self.plan_details)
            # column = ["Partner","Date","Company Name","Customer Name","Plan cost (After Tax)","Salary cover","Expense cover"]
            column = ["Partner","date","Company Name","Customer Name","Plan cost (After Tax)","Salary cover"]

            allpolicyData['Partner'] = allpolicyData['partnerName']
            allpolicyData['createdAt'] = allpolicyData['date']
            allpolicyData['Company Name'] = allpolicyData['companyName']
            allpolicyData['Customer Name'] = allpolicyData['customerName']
            allpolicyData['Plan cost (After Tax)']= allpolicyData['premiumWithINR']
            allpolicyData['Salary cover']= allpolicyData['salaryCoverWithINR']
            # allpolicyData['Expense cover']= allpolicyData['expenseCoverWithINR']
            allpolicyData = allpolicyData[column]
            allpolicyData = allpolicyData.to_dict("records")
            # allpolicyData.sort(key=lambda x: datetime.strptime(x['createdAt'], "%Y-%m-%d"),reverse=True)
            allpolicyData.sort( key = lambda x: x['date'],reverse=True)
            allPolicyDataTable = dash_table.DataTable(
                allpolicyData,
                [{"name": i, "id": i} for i in column],
                page_size=10,
                style_cell={"textAlign": "left"},
                style_header=self.headerStyle,
                style_data=self.dataStyle,
                style_data_conditional=self.dataCondition,
            )
            toc = time.perf_counter()
            print(f"DashDetailsController getAllPoliciesDataTable executed in {toc - tic:0.4f} seconds")
            return allPolicyDataTable
    
        

    def getPoliciesByCities(self):
        if self.plan_details == []:
            cityDataTable = html.H6("*******we are yet to open up********")
            return cityDataTable
        else:
            tic = time.perf_counter()
            cities = [x['city'] for x in self.plan_details if x.get('city')]
            citywisedata = []
            for i in list(set(cities)):
                plan = [x for x in  self.plan_details if x.get('city') == i]
                if plan:
                    city_data = dict(City = plan[0]['city'],planCount = len(plan),planCost = locale.currency(sum(float(x['netAmount']) for x in plan),grouping=True),salaryCover = locale.currency(sum(float(x['salaryCover']) for x in plan),grouping=True), expenseCover = locale.currency(sum(float(x['expenseCover']) for x in plan),grouping=True))
                    cityHitCount = self.quote_merged_with_state_city[self.quote_merged_with_state_city['city'] == plan[0]['city']]
                    cityUniqueHitCount = cityHitCount.mobile.unique()
                    city_data["strikeRate"] = round((len(plan)/len(cityHitCount))*100,2)
                    city_data["strikeRateUnique"] = round((len(plan)/len(cityUniqueHitCount))*100,2)
                    citywisedata.append(city_data)
            citywisedata.sort(key=lambda x: x["planCount"],reverse=True)
            policiesByCity = pd.DataFrame(citywisedata)  
            policiesByCity.rename(columns={"planCount": "Plan Count"}, inplace=True)
            policiesByCity.rename(columns={"planCost": "Plan Cost(after Tax)"}, inplace=True)
            policiesByCity.rename(columns={"salaryCover": "Salary Cover"}, inplace=True)
            policiesByCity.rename(columns={"expenseCover": "Expense Cover"}, inplace=True)
            policiesByCity.rename(columns={"strikeRate": "Strike Rate"}, inplace=True)
            policiesByCity.rename(columns={"strikeRateUnique": "Strike Rate Unique"}, inplace=True)
            # policiesByCity['strikeRate'] = round((policiesByCity['policyCount']/policiesByCity['companyHitCount'])*100,2)
            cityDataTable = dash_table.DataTable(
                policiesByCity.to_dict("records"),
                [{"name": i, "id": i} for i in policiesByCity.columns],
                page_size=10,
                style_cell={"textAlign": "left"},
                style_header=self.headerStyle,
                style_data=self.dataStyle,
                style_data_conditional=self.dataCondition,
            )
            toc = time.perf_counter()
            print(f"DashDetailsController getPoliciesByCities executed in {toc - tic:0.4f} seconds")
            return cityDataTable

    def getPoliciesByState(self):
        if self.plan_details == []:
            stateDataTable = html.H6("*******we are yet to open up********")
            return stateDataTable
        else:
            tic = time.perf_counter()
            states = [x['state'] for x in self.plan_details]
            statewisedata = []
            for i in list(set(states)):
                plan = [x for x in  self.plan_details if x['state'] == i]
                # active_policy_by_city = len([x for x in plan if x['status'] == "Active"])
                if plan:
                    state_data = dict(State = plan[0]['state'],planCount = len(plan),planCost = locale.currency(sum(float(x['netAmount']) for x in plan),grouping=True),salaryCover = locale.currency(sum(float(x['salaryCover']) for x in plan),grouping=True), expenseCover = locale.currency(sum(float(x['expenseCover']) for x in plan),grouping=True))
                    stateHitCount = self.quote_merged_with_state_city[self.quote_merged_with_state_city['stateCode'] == plan[0]['state']]
                    stateUniqueHitCount = stateHitCount.mobile.unique()
                    # print(i,"plans issues -> ",(len(plan)),"\n total state count-> ",len(stateHitCount))
                    state_data["strikeRate"] = round((len(plan)/len(stateHitCount))*100,2)
                    state_data["strikeRateUnique"] = round((len(plan)/len(stateUniqueHitCount))*100,2)
                    statewisedata.append(state_data)
            statewisedata.sort(key=lambda x: x["planCount"],reverse=True)  
            policiesByState = pd.DataFrame(statewisedata)  
            policiesByState.rename(columns={"planCount": "Plan Count"}, inplace=True)
            policiesByState.rename(columns={"planCost": "Plan Cost(after Tax)"}, inplace=True)
            policiesByState.rename(columns={"salaryCover": "Salary Cover"}, inplace=True)
            policiesByState.rename(columns={"expenseCover": "Expense Cover"}, inplace=True)
            policiesByState.rename(columns={"strikeRate": "Strike Rate"}, inplace=True)
            policiesByState.rename(columns={"strikeRateUnique": "Strike Rate Unique"}, inplace=True)
            stateDataTable = dash_table.DataTable(
                policiesByState.to_dict("records"),
                [{"name": i, "id": i} for i in policiesByState.columns],
                page_size=10,
                style_cell={"textAlign": "left"},
                style_header=self.headerStyle,
                style_data=self.dataStyle,
                style_data_conditional=self.dataCondition,
            )
            toc = time.perf_counter()
            print(f"DashDetailsController getPoliciesByState executed in {toc - tic:0.4f} seconds")
            return stateDataTable

    def getGenderDataTable(self):
        tic = time.perf_counter()
        if self.plan_details == []:
            genderDataTable = html.H6("*******we are yet to open up********")
        else:
            allpolicyDataPinStateCompany  = pd.DataFrame(self.plan_details)
            policiesByGender = (
                allpolicyDataPinStateCompany.groupby(["gender"])
                # .agg({"proposalId": "count", "netAmount": "sum", "salaryCover": "sum", "expenseCover": "sum"})
                .agg({"proposalId": "count", "netAmount": "sum", "salaryCover": "sum"})

                .reset_index()
            )
            policiesByGender.rename(columns={"netAmount": "Plan cost(after Tax)","salaryCover": "Salary cover","expenseCover": "Expense Cover","proposalId": "Plan count"}, inplace=True)
            # policiesByGender.rename(columns={"salaryCover": "Salary cover"}, inplace=True)
            # policiesByGender.rename(columns={"expenseCover": "Expense Cover"}, inplace=True)
            # policiesByGender.rename(columns={"proposalId": "Plan count"}, inplace=True)
            gender =  policiesByGender.to_dict("records")
            gender.sort(key=lambda x: x["Plan count"],reverse=True)  
            for i in  gender:
                i['Plan cost(after Tax)'] = locale.currency(round(float(i['Plan cost(after Tax)']),2),grouping=True)
                i['Salary cover'] = locale.currency(round(float(i['Salary cover']),2),grouping=True)
                # i['Expense Cover'] = locale.currency(round(float(i['Expense Cover']),2),grouping=True)
            # print(gender)

            genderDataTable = dash_table.DataTable(
                gender,
                [{"name": i, "id": i} for i in policiesByGender.columns],
                page_size=10,
                style_cell={"textAlign": "left"},
                style_header=self.headerStyle,
                style_data=self.dataStyle,
                style_data_conditional=self.dataCondition,
            )
        toc = time.perf_counter()
        print(f"DashDetailsController getGenderDataTable executed in {toc - tic:0.4f} seconds")
        return genderDataTable

    def getPlanDataTable(self):
        if self.plan_details == []:
            planDataTable = html.H6("*******we are yet to open up********")
        else:
            tic = time.perf_counter()
            allpolicyDataPinStateCompany  = pd.DataFrame(self.plan_details)
            policiesByPlanAmount = (
                # allpolicyDataPinStateCompany.groupby(["salaryCover","expenseCover","policyAmount"])
                allpolicyDataPinStateCompany.groupby(["salaryCover"])


                .agg({"proposalId": "count", "netAmount": "sum"})
                .reset_index()
            )
            # print(policiesByPlanAmount)
            policiesByPlanAmount.reset_index(inplace=True)
            # print(policiesByPlanAmount)
            # policiesByPlanAmount["planCount"] = policiesByPlanAmount["salaryCover"].apply(self.repo.getPlanAmountCount)
            # policiesByPlanAmount["planCountUniq"] = policiesByPlanAmount["salaryCover"].apply(self.repo.getPlanUniqueAmountCount)
            # policiesByPlanAmount["Strike Rate"] = round(policiesByPlanAmount["proposalId"] * 100 / policiesByPlanAmount["planCount"],2)
            # policiesByPlanAmount["Strike Rate Uniq"] = round(policiesByPlanAmount["proposalId"] * 100 / policiesByPlanAmount["planCountUniq"],2)
            policiesByPlanAmount.rename(columns={"salaryCover": "Salary cover"}, inplace=True)
            # policiesByPlanAmount.rename(columns={"expenseCover": "Expense Cover"}, inplace=True)
            policiesByPlanAmount.rename(columns={"proposalId": "Plan count"}, inplace=True)
            policiesByPlanAmount.rename(columns={"netAmount": "Plan cost(after Tax)"}, inplace=True)
            # policiesByPlanAmount = policiesByPlanAmount[["Salary cover","Expense Cover","Plan count","Plan cost(after Tax)","Strike Rate","Strike Rate Uniq"]]
            # policiesByPlanAmount = policiesByPlanAmount[["Salary cover","Plan count","Plan cost(after Tax)","Strike Rate","Strike Rate Uniq"]]
            policiesByPlanAmount = policiesByPlanAmount[["Salary cover","Plan count","Plan cost(after Tax)"]]

            policiesByPlanAmount = policiesByPlanAmount.sort_values(by = "Plan count", ascending = False)
            policiesByPlanAmount['Plan cost(after Tax)'] = policiesByPlanAmount['Plan cost(after Tax)'].apply(lambda x: locale.currency(float(x) if x > 0 else x))
            policiesByPlanAmount['Salary cover'] = policiesByPlanAmount['Salary cover'].apply(lambda x: locale.currency(float(x) if x > 0 else x))

            plancount = policiesByPlanAmount.to_dict("records")
            # plancount.sort(key=lambda x: x["Plan count"],reverse=True)  
            # for i in  plancount:
            #     i['Plan cost(after Tax)'] = locale.currency(round(float(i['Plan cost(after Tax)']),2),grouping=True)
            #     i['Salary cover'] = locale.currency(round(float(i['Salary cover']),2),grouping=True)
                # i['Expense Cover'] = locale.currency(round(float(i['Expense Cover']),2),grouping=True)
            planDataTable = dash_table.DataTable(
               plancount,
                [{"name": i, "id": i} for i in policiesByPlanAmount.columns],
                page_size=10,
                style_cell={"textAlign": "left"},
                style_header=self.headerStyle,
                style_data=self.dataStyle,
                style_data_conditional=self.dataCondition,
            )
            toc = time.perf_counter()
            print(f"DashDetailsController getPlanDataTable executed in {toc - tic:0.4f} seconds")
            return planDataTable

    def getCreditDataTable(self):
        if not self.plan_details == []:
            tic = time.perf_counter()
            credit_scores = [x['creditScore'] for x in self.plan_details]
            bins = [x for x in range(min(credit_scores), max(credit_scores)+50, 50)]
            labels = list(string.ascii_uppercase)
            creditScoreFrame = pd.DataFrame({"creditScore": credit_scores})

            creditScoreFrame["category"] = pd.cut(creditScoreFrame["creditScore"], bins, labels)

            creditBuckets = creditScoreFrame.groupby("category").count().reset_index()

            creditBuckets = pd.DataFrame(creditBuckets[creditBuckets["creditScore"] != 0])
            creditBuckets["category"] = creditBuckets["category"].astype(str)
            creditBuckets.rename(columns={"category": "Credi score band"}, inplace=True)
            creditBuckets.rename(columns={"creditScore": "Plan count"}, inplace=True)
            # print(creditBuckets)
            creditDataTable = dash_table.DataTable(
                creditBuckets.to_dict("records"),
                [{"name": i, "id": i} for i in creditBuckets.columns],
                style_cell={"textAlign": "left"},
                style_header=self.headerStyle,
                style_data=self.dataStyle,
                style_data_conditional=self.dataCondition,
            )
            toc = time.perf_counter()
            print(f"DashDetailsController getCreditDataTable executed in {toc - tic:0.4f} seconds")
            # page_size=10,)
        else:
            creditDataTable = html.H6("*******we are yet to open up********")
        return creditDataTable
        

    def get_overall_summary_table(self):
        tic = time.perf_counter()
        self.overall_summary_table_data.sort(key=lambda x: x["Plan Converted Count"],reverse=True)  
        self.overall_summary_table_data.append({"Partner":"Total","Lead Count":len(self.lead_details),"Quote count":sum(x['Quote count'] for x in self.overall_summary_table_data),"Proposals Count":sum(x['Proposals Count'] for x in self.overall_summary_table_data),
        "Payments Count":sum(x['Payments Count'] for x in self.overall_summary_table_data),"Plan Converted Count":sum(x['Plan Converted Count'] for x in self.overall_summary_table_data),"Strike Rate":round((sum(x['Plan Converted Count'] for x in self.overall_summary_table_data)/sum(x['Quote count'] for x in self.overall_summary_table_data)) * 100,2)})
        policy_summary = pd.DataFrame(self.overall_summary_table_data)
        policy_summary_table = dash_table.DataTable(
            policy_summary.to_dict("records"),
            [{"name": i, "id": i} for i in policy_summary.columns],
            page_size=10,
            style_cell={"textAlign": "left"},
            style_header=self.headerStyle,
            style_data=self.dataStyle,
            style_data_conditional=self.dataCondition,
        )
        toc = time.perf_counter()
        print(f"DashDetailsController get_overall_summary_table executed in {toc - tic:0.4f} seconds")
        return policy_summary_table

    def get_overall_summary_unique_table(self):
        tic = time.perf_counter()
        self.overall_summary_table_unique_data.sort(key=lambda x: x["Plan Converted Count"],reverse=True)  
        self.overall_summary_table_unique_data.append({"Partner":"Total","Lead Count":len(self.lead_details),"Quote count":sum(x['Quote count'] for x in self.overall_summary_table_unique_data),"Proposals Count":sum(x['Proposals Count'] for x in self.overall_summary_table_unique_data),
        "Payments Count":sum(x['Payments Count'] for x in self.overall_summary_table_unique_data),"Plan Converted Count":sum(x['Plan Converted Count'] for x in self.overall_summary_table_unique_data),"Strike Rate":round((sum(x['Plan Converted Count'] for x in self.overall_summary_table_unique_data)/sum(x['Quote count'] for x in self.overall_summary_table_unique_data)) * 100,2)})
        policy_summary_unique = pd.DataFrame(self.overall_summary_table_unique_data)
        policy_summary_unique_table = dash_table.DataTable(
            policy_summary_unique.to_dict("records"),
            [{"name": i, "id": i} for i in policy_summary_unique.columns],
            page_size=10,
            style_cell={"textAlign": "left"},
            style_header=self.headerStyle,
            style_data=self.dataStyle,
            style_data_conditional=self.dataCondition,
        )
        toc = time.perf_counter()
        print(f"DashDetailsController get_overall_summary_unique_table executed in {toc - tic:0.4f} seconds")
        return policy_summary_unique_table
    
    def get_cancellation_details(self):
        tic = time.perf_counter()
        table_data = []
        for i in self.client_list:
            cancelled_plan = self.repo.cancelled_plan_count(i.get('clientId'))
            quote = self.repo.partner_quote(i.get('clientId'))
            dict = {
                "Partner": i.get('code').title(),
                "No Experion":"NA",
                "Not Eligible": len([x for x in quote if len(x['response']['plans']) == 0]),
                "Cancellation": cancelled_plan,
            }
            table_data.append(dict)
        table_data.append({"Partner":"Total","No Experion":"NA","Not Eligible":sum(x['Not Eligible'] for x in table_data),"Cancellation":sum(x['Cancellation'] for x in table_data)})
        cacellation_summary = pd.DataFrame(table_data)
        calcellation_summary_table = dash_table.DataTable(
            cacellation_summary.to_dict("records"),
            [{"name": i, "id": i} for i in cacellation_summary.columns],
            page_size=10,
            style_cell={"textAlign": "left"},
            style_header=self.headerStyle,
            style_data=self.dataStyle,
            style_data_conditional=self.dataCondition,
        )
        toc = time.perf_counter()
        print(f"DashDetailsController get_cancellation_details executed in {toc - tic:0.4f} seconds")
        return calcellation_summary_table
