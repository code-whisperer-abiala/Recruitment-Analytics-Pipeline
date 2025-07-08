# Recruitment Analytics Pipeline – SQL to Power BI

An end-to-end recruitment analytics project that simulates a real-world hiring workflow from database design to dashboard insights. Built entirely from scratch using **PostgreSQL**, **Python**, **Power BI**, and **DAX**, this project tracks key talent acquisition metrics like application trends, recruiter performance, and funnel conversion — all in one interactive dashboard.

---

##  Tech Stack

- **PostgreSQL** (DDL, DML, relational schema design)
- **Python** (`psycopg2`, `Faker`, `argparse` for data simulation and inserts)
- **Power BI** (Power Query, DAX, model view, drillthroughs)
- **ODBC Connector** (to pull PostgreSQL data into Power BI)

---

##  Key Features

###  SQL + Python Integration
- Designed relational tables for `Candidates`, `CandidateStatus`, and `StatusLog`
- Used Python + `psycopg2` to simulate 200+ synthetic records
- Parameterized the script with `argparse` for CLI flexibility

###  Power BI Dashboard
- Custom DAX metrics for application-to-hire ratios, time-to-hire, and recruiter KPIs
- Power Query transformations for weekly trend labeling, date normalization
- Conditional formatting, funnel visuals, and slicer-friendly layout

###  Full Data Journey
- Raw inserts from CSV and Python → PostgreSQL
- ODBC pipeline → Power BI model → interactive dashboard

---

## Dashboard Overview

**Page 1: Recruitment Overview**
- KPI cards (Applications, Hires, Roles)
- Applications vs 3-month rolling average
- Monthly hires vs benchmark
- Application-to-hire ratio gauge

**Page 2: Funnel Insights**
- Funnel chart (Applied → Interviewing → Offer → Hired)
- Interview-to-hire rate, Offer acceptance rate
- Role-wise status matrix

**Page 3: Recruiter Drilldown**
- Avg. time to hire by recruiter
- Conditional color logic for performance
- Pivoted recruiter output matrix
- Target vs actual hires per recruiter

---

## Video Walkthrough

Want a guided tour of the full project?  
Watch the Loom walkthrough below:

▶️ [Watch the Full Walkthrough on Loom](https://www.loom.com/share/f4f71b9a912b488ca82c18e5634aee7d?sid=1483eb4c-89f1-4e5f-82be-7c57f84b1ed4)

---

##  Meet the Analyst

Hi, I’m **Grace Isiaka** — a Business Intelligence Analyst who enjoys designing systems that make data work for people.

I started in operations and admin, where I constantly saw how hard it was to get clear insights from messy spreadsheets and disconnected systems. That’s what drove me into data analytics — to bring clarity, structure, and storytelling to everyday decision-making.

I recently completed a **Graduate Certificate in Business Analytics from Seneca Polytechnic**, where I focused on **predictive modeling, NLP, dashboard design,** and **workflow automation**.

I built this project as a full-stack case study: from database schema to realistic hiring simulation, all the way through to dashboard interactivity and drillthrough-ready metrics.

### What I care about:
- Making data clear, not just complex
- Building tools real people can use
- Combining backend logic with frontend storytelling
- Bringing projects from idea → insight → impact

 Want to connect or collaborate?  
Let’s build something insightful: [LinkedIn](https://www.linkedin.com/in/grace-isiaka)

---

##  Tags
`#PowerBI` `#PostgreSQL` `#Python` `#RecruitmentAnalytics` `#FullStackDashboard` `#SQLtoInsights`
