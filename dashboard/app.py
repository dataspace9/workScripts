# -*- coding: utf-8 -*-

"""
Created on Thu Mar 23 20:01:02 2023

@author: sheld
"""

import os,locale
import mysql.connector

from dash import Dash, html
from flask import Flask, redirect, render_template, request, session, url_for
from pymongo import MongoClient

from controllers.dashDetailsController import DashDetailsController

# valid_username_password_pairs = {"jon":"doe"}

server = Flask(__name__)
# app = Dash(__name__)
app = Dash(__name__, server=server, url_base_pathname="/home/")
locale.setlocale(locale.LC_MONETARY, 'en_IN')


if os.environ.get("flag"):
    env = os.environ.get("flag")
    server.config.from_pyfile(f"./{env}_settings.cfg")
    server.logger.debug(f"##############  {env.upper()} ENV ##############")
else:
    env = "uat"
    server.config.from_pyfile(f"./{env}_settings.cfg")
    server.logger.debug(f"##############  {env.upper()} ENV ##############")

server.config["DB_CON"] = MongoClient(server.config["MONGODB_URI"], maxPoolSize=300)

server.config["MASTER_DB"] = server.config["DB_CON"][server.config["MASTER_DB"]]
server.config["CLIENT_DB"] = server.config["DB_CON"][server.config["CLIENT_DB"]]
server.config["ASSURE_DASH_DB"] = server.config["DB_CON"][server.config["ASSURE_DASH_DB"]]
server.config["MYSQL_CUSTOMER_DB"] = mysql.connector.connect(host=server.config["MYSQL_HOST"],user=server.config["MYSQL_USERNAME"],password=server.config["MYSQL_PASSWORD"],database="customer-service-db")

users = [

]


@server.route("/")
def home():
    if "username" in session:
        # return f"Hello {session['username']}! <a href='/logout'>Logout</a>"
        return app.index()
    else:
        return redirect(url_for("login"))


@server.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form["username"]
        password = request.form["password"]
        for user in users:
            if user["username"] == username and user["password"] == password:
                session["username"] = username
                return redirect(url_for("home"))
        return "Invalid username or password. Please try again."
    else:
        return render_template('login.html')
    
@server.route("/home/")
def my_dash_app():
    return app.index()


def serve_layout():
    ctrl = DashDetailsController(server.config["CLIENT_DB"], server.config["MASTER_DB"],server.config['ASSURE_DASH_DB'],server.config["MYSQL_CUSTOMER_DB"])
    return html.Div(
        [
            html.H1(children="Income Protect Dashboard", style=ctrl.style),
            html.Div(
                [
                    html.H4(children="Daily Summary(Today's Activity)", style=ctrl.style),
                    ctrl.plans_created_today(),
                    html.H4(children="Plan Issued Summary", style=ctrl.style),
                    ctrl.plans_created_till_now(),
                    html.H4(children="Income protect overall summary", style=ctrl.style),
                    ctrl.get_overall_summary_table(),
                    html.H4(children="Income protect overall summary(Unique)", style=ctrl.style),
                    ctrl.get_overall_summary_unique_table(),
                    # html.H4(children="Lead Rejection summary", style=ctrl.style),
                    # ctrl.get_cancellation_details(),
                    html.H4(children="Converted Plan Detail", style=ctrl.style),
                    ctrl.getAllPoliciesDataTable(),
                    # html.H4(children="Company State Data", style=ctrl.style),
                    # ctrl.getPoliciesByState(),
                    #  html.H4(children="Customer work City Overview", style=ctrl.style),
                    ctrl.getPoliciesByCities(),
                    html.H4(children="Gender wise overview", style=ctrl.style),
                    ctrl.getGenderDataTable(),
                    html.H4(children="Plan wise policy data", style=ctrl.style),
                    ctrl.getPlanDataTable(),
                    html.H4(children="Credit Scores Data Of Plans Issued", style=ctrl.style),
                    ctrl.getCreditDataTable(),
                    html.H4(children ="Credit Scores Data Of All Users",style = ctrl.style),
                    ctrl.getAllCreditScoresDataTable(),
                    ctrl.getCharts(),
                    # ctrl.getCreditScoresGraph(),
                    # html.H4(children="Daily lead count", style=ctrl.style),
                    # ctrl.getDailyLeadChart(),
                    # html.H4(children="Daily increase in count", style=ctrl.style),
                    # ctrl.getDailyLeadDifferentChart(),
                    # html.H4(children="Daily increase %", style=ctrl.style),
                    # ctrl.getDailyLeadPercentageChart(),
                ]
            ),
        ]
    )


app.layout = serve_layout

if __name__ == "__main__":
    app.run_server(debug=True, use_reloader=True)
