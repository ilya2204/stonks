import pandas as pd
import requests
import os
import threading
from bs4 import BeautifulSoup as bs
from tqdm.notebook import tqdm


class Parser:
    def __init__(self, username, password):
        self.url = 'https://ycharts.com/companies/{ticker}/{key_stat}.json?endDate=01%2F23%2F2030&pageNum={pageNum}&startDate=12%2F12%2F1900'
        self.s = requests.Session()
        self.main_data = dict()
        data = {'csrfmiddlewaretoken': '1dDdvLKmoczbTOCoKQBNV0GXoFyrOEpdEXLVdJ6KH79rU6hlxbhCcwJDmIHwzXyl',
                'username': username,
                'password': password,
                'next': '/companies/AAPL/key_stats',
                "login_username": "my_login", "login_password": "my_password"}
        url = "https://ycharts.com/login?next=/companies/AAPL/key_stats"
        r = self.s.post(url, data=data, headers={'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/79.0.3945.79 Chrome/79.0.3945.79 Safari/537.36',
                                                 'Host': 'ycharts.com',
                                                 'Content-Length': '172',
                                                 'Cookie': 'd-a8e6=58463237-5d1d-4265-b96c-9d4025469380; __utmc=69688216; __utmz=69688216.1579888227.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); wcsid=6qL2z9tz2rfNPtrm1y8Lx0MaBHbWjd3o; hblid=nS7ziej4jcCteM2l1y8Lx0MadrvW5bBH; _okdetect=%7B%22token%22%3A%2215798882279140%22%2C%22proto%22%3A%22https%3A%22%2C%22host%22%3A%22ycharts.com%22%7D; hubspotutk=2a2b614678d6963134d04b1b1aadd7a3; __hssrc=1; olfsk=olfsk699086017550731; _ok=1228-592-10-8601; 33e807c05af9078f6b2ed01ced5fc28d5c8f52f4=1; _okbk=cd5%3Daway%2Ccd4%3Dtrue%2Cwa1%3Dfalse%2Cvi5%3D0%2Cvi4%3D1579888231039%2Cvi3%3Dactive%2Cvi2%3Dfalse%2Cvi1%3Dfalse%2Ccd8%3Dchat%2Ccd6%3D0%2Ccd3%3Dfalse%2Ccd2%3D0%2Ccd1%3D0%2C; _okac=72697bf5ceb710d933c9635041e4a8ca; _okla=1; __utma=69688216.1810260626.1579888227.1579906621.1579909736.5; __hstc=165832289.2a2b614678d6963134d04b1b1aadd7a3.1579888227928.1579906621208.1579909737051.5; csrftoken=cZ7NxgfhO8YJONpHgybkxkdZjMVRkpXPPJfvfeBF73yZP54E3TR9OQgFhP4W5I6X; ycsessionid=7ofs45ukiqaxz206jtqtbf0ow6y31vjb; s-9da4=eafabce6-57c0-4db0-b163-911290ec115c; __utmt=1; page_view_ctr=18; __utmb=69688216.10.10.1579909736; mp_bd6455515e9730c7dc2f008755a4ddfe_mixpanel=%7B%22distinct_id%22%3A%20%2216fd6d00dfb698-090a20db04526b-39647b0e-13c680-16fd6d00dfce33%22%2C%22%24device_id%22%3A%20%2216fd8ad332177-0e1412d80b2fb2-24414032-1fa400-16fd8ad33222a5%22%2C%22%24initial_referrer%22%3A%20%22%24direct%22%2C%22%24initial_referring_domain%22%3A%20%22%24direct%22%2C%22__mps%22%3A%20%7B%7D%2C%22__mpso%22%3A%20%7B%7D%2C%22__mpus%22%3A%20%7B%7D%2C%22__mpa%22%3A%20%7B%7D%2C%22__mpu%22%3A%20%7B%7D%2C%22__mpr%22%3A%20%5B%5D%2C%22__mpap%22%3A%20%5B%5D%2C%22%24user_id%22%3A%20%2216fd6d00dfb698-090a20db04526b-39647b0e-13c680-16fd6d00dfce33%22%7D; _oklv=1579911199267%2C6qL2z9tz2rfNPtrm1y8Lx0MaBHbWjd3o; __hssc=165832289.10.1579909737051',
                                                 'Referer': 'https://ycharts.com/login?next=%2Fcompanies%2FAAPL%2Fkey_stats',
                                                 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                                                 'Accept-Encoding': 'gzip, deflate, br',
                                                 'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
                                                 'Cache-Control': 'max-age=0',
                                                 'Connection': 'keep-alive',
                                                 'Sec-Fetch-Mode': 'navigate',
                                                 'Sec-Fetch-Site': 'same-origin',
                                                 'Origin': 'https://ycharts.com'})
        if r.status_code == 404:
            print('Error')

        self.headers = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                        'Accept-Encoding': 'gzip, deflate, br',
                        'Accept-Language': 'en-US,en;q=0.5',
                        'Connection': 'keep-alive',
                        'Host': 'ycharts.com',
                        'Upgrade-Insecure-Requests': '1',
                        'X-Frame-Options': 'SAMEORIGIN',
                        'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:72.0) Gecko/20100101 Firefox/72.0',
                        'X-CSRFToken': 'moEPpEsiNx9uYgWqXDHQ5RPckv3H9Rmuh5tK99MoX1mNc2kCDpS0dkTykG5vAEI8',
                        'X-Requested-With': 'XMLHttpRequest'}
        self.key_stats = {'1 Month Price Returns (Daily)': 'one_month_return',
                          '3 Month Price Returns (Daily)': 'three_month_return',
                          '6 Month Price Returns (Daily)': 'six_month_return',
                          'Year to Date Price Returns (Daily)': 'ytd_return',
                          '1 Year Price Returns (Daily)': 'one_year_return',
                          '3 Year Price Returns (Daily)': 'three_year_return',
                          '52 Week High (Daily)': 'year_high',
                          '52 Week Low (Daily)': 'year_low',
                          'Shares Outstanding': 'shares_outstanding',
                          'Dividend': 'dividend',
                          'Dividend Yield': 'dividend_yield',
                          'Cash Dividend Payout Ratio': 'cash_dividend_payout_ratio',
                          'Payout Ratio': 'payout_ratio',
                          'Beta (5Y)': 'market_beta_60_month',
                          'Max Drawdown (All)': 'max_drawdown_all',
                          'Daily Value at Risk (VaR) 1% (All)': 'historical_daily_var_1_all',
                          'Daily Value at Risk (VaR) 5% (All)': 'historical_daily_var_5_all',
                          'Monthly Value at Risk (VaR) 5% (All)': 'historical_monthly_var_5_all',
                          'Monthly Value at Risk (VaR) 1% (All)': 'historical_monthly_var_1_all',
                          'Revenue Estimates for Current Quarter': 'sales_est_0q',
                          'Revenue Estimates for Current Fiscal Year': 'sales_est_0y',
                          'EPS Estimates for Current Quarter': 'eps_est_0q',
                          'EPS Estimates for Current Fiscal Year': 'eps_est_0y',
                          'PE Ratio (Forward)': 'forward_pe_ratio',
                          'PE Ratio (Forward 1y)': 'forward_pe_ratio_1y',
                          'PS Ratio (Forward)': 'forward_ps_ratio',
                          'PS Ratio (Forward 1y)': 'forward_ps_ratio_1y',
                          'Price Target Upside (Daily)': 'price_target_upside',
                          'Revenue (TTM)': 'revenues_ttm',
                          'Revenue (Per Share Quarterly)': 'revenues_per_share',
                          'Revenue (Quarterly YoY Growth)': 'revenues_growth',
                          'EPS Diluted (TTM)': 'eps_ttm',
                          'EPS Diluted (Quarterly YoY Growth)': 'eps_growth',
                          'Net Income (TTM)': 'net_income_ttm',
                          'EBITDA (TTM)': 'ebitda_ttm',
                          'Total Assets (Quarterly)': 'assets',
                          'Cash and Short Term Investments (Quarterly)': 'cash_on_hand',
                          'Book Value (Per Share)': 'book_value_of_equity_per_share',
                          'Tangible Book Value (Per Share)': 'book_value_of_tangible_equity_per_share',
                          'Total Liabilities (Quarterly)': 'liabilities',
                          'Non-Current Portion of Long Term Debt (Quarterly)': 'long_term_debt',
                          'Total Long Term Debt (Quarterly)': 'total_long_term_debt',
                          'Shareholders Equity (Quarterly)': 'shareholders_equity',
                          'Cash from Financing (TTM)': 'cash_financing_ttm',
                          'Cash from Investing (TTM)': 'cash_investing_ttm',
                          'Cash from Operations (TTM)': 'cash_operations_ttm',
                          'Capital Expenditures (TTM)': 'capex_ttm',
                          'Net Income (% of Quarterly Revenues)': 'net_income_cs_rev',
                          'Net Income (% of Annual Revenues)': 'net_income_annual_cs_rev',
                          'Accruals (Quarterly)': 'accruals',
                          'Beneish M-Score (Annual)': 'beneish_m_score',
                          'Gross Profit Margin (Quarterly)': 'gross_profit_margin',
                          'Profit Margin (Quarterly)': 'profit_margin',
                          'EBITDA Margin (TTM)': 'ebitda_margin_ttm',
                          'Operating Margin (TTM)': 'operating_margin_ttm',
                          'Asset Utilization (TTM)': 'asset_utilization',
                          'Days Sales Outstanding (Quarterly)': 'days_sales_outstanding',
                          'Days Inventory Outstanding (Quarterly)': 'days_inventory_outstanding',
                          'Days Payable Outstanding (Quarterly)': 'days_payables_outstanding',
                          'Receivables Turnover (Quarterly)': 'receivables_turnover',
                          'Return on Assets': 'return_on_assets',
                          'Return on Equity': 'return_on_equity',
                          'Return on Invested Capital': 'return_on_invested_capital',
                          'Market Cap': 'market_cap',
                          'Enterprise Value': 'enterprise_value',
                          'PE Ratio': 'pe_ratio',
                          'PE 10': 'pe_10',
                          'PEG Ratio': 'peg_ratio',
                          'Earnings Yield': 'earning_yield',
                          'PS Ratio': 'ps_ratio',
                          'Price to Book Value': 'price_to_book_value',
                          'EV to Revenues': 'ev_revenues',
                          'EV to EBITDA': 'ev_ebitda',
                          'EV to EBIT': 'ev_ebit',
                          'Operating PE Ratio': 'operating_pe_ratio',
                          'Operating Earnings Yield': 'operating_earning_yield',
                          'Altman Z-Score (TTM)': 'altman_z_score',
                          'Current Ratio': 'current_ratio',
                          'Debt to Equity Ratio': 'debt_equity_ratio',
                          'Free Cash Flow (Quarterly)': 'free_cash_flow',
                          'KZ Index (Annual)': 'kz_index',
                          'Tangible Common Equity Ratio (Quarterly)': 'tangible_common_equity_ratio',
                          'Times Interest Earned (TTM)': 'times_interest_earned',
                          'Total Employees (Annual)': 'total_employee_number',
                          'Net Income Per Employee (Annual)': 'ni_per_employee_annual',
                          'CA Score (TTM)': 'ca_score',
                          'Piotroski F Score (TTM)': 'f_score_ttm',
                          'Fulmer H Factor (TTM)': 'fulmer_h_score',
                          "Graham's Number (TTM)": 'graham_number',
                          'Net Current Asset Value Per Share (NCAVPS) (Quarterly)': 'ncavps',
                          'Ohlson Score (TTM)': 'ohlson_score',
                          'Quality Ratio (TTM)': 'quality_ratio',
                          'Springate Score (TTM)': 'springate_score',
                          'Sustainable Growth Rate (TTM)': 'sustainable_growth_rate',
                          "Tobin's Q (Approximate) (Quarterly)": 'tobin_q',
                          'Zmijewski Score (TTM)': 'zmijewski_score',
                          'Momentum Score': 'momentum_fractile',
                          'Market Cap Score': 'market_cap_fractile',
                          'Quality Ratio Score': 'quality_ratio_fractile',
                          'Revenue Per Employee (Annual)' : 'revenue_per_employee_annual'}

        self.skips = {
            "Basic EPS (Quarterly)",
            "Diluted EPS (Quarterly)",
            "Income (Quarterly)",
            "SEC Filing Links",
            "Shares Data (Quarterly)", "Cash Flow - Operations (Quarterly)",
            "Cash Flow - Investing (Quarterly)",
            "Cash Flow - Financing (Quarterly)",
            "Ending Cash (Quarterly)",
            "Additional Items (Quarterly)",
            "SEC Filing Links",
            "Assets (Quarterly)",
            "Liabilities (Quarterly)",
            "Shareholder's Equity (Quarterly)",
            "SEC Filing Links",
        }
        
        self.financials_stats = {
            'balance_sheet',
            'income_statement',
            'cash_flow_statement'
        }
        self.price_data = dict()

    def get_last_page(self, company_name, key_stat):
        u = self.url.format(ticker=company_name, key_stat=key_stat, pageNum=1)
        r = self.s.get(u, headers=self.headers)
        return r.json()['last_page_num']

    def get_data(self, company_name, key_stat, pageNum):
        u = self.url.format(ticker=company_name,
                            key_stat=key_stat, pageNum=pageNum)
        r = self.s.get(u, headers=self.headers)
        html = r.json()['data_table_html']
        soup = bs(html, features="lxml")
        if key_stat != 'dividend':
            col1 = [x.text for x in soup.findAll("td", {"class": "col1"})]
            col2 = [x.text.split()[0] if len(x.text.split())
                    else '' for x in soup.findAll("td", {"class": "col2"})]
            data = [[col1[i], col2[i]] for i in range(len(col1))]
        else:
            col1 = [x.text for x in soup.find_all("td", {"class": "col1"})]
            col2 = [x.text for x in soup.find_all("td", {"class": "col2"})]
            col3 = [x.text for x in soup.find_all("td", {"class": "col3"})]
            col4 = [x.text for x in soup.find_all("td", {"class": "col4"})]
            col5 = [x.text for x in soup.find_all("td", {"class": "col5"})]
            col6 = [x.text for x in soup.find_all("td", {"class": "col6"})]
            data = [[col1[i], col2[i], col3[i], col4[i], col5[i], col6[i]]
                    for i in range(len(col1))]
        return data

    def get_key_stat(self, company_name, key_stat):
        if key_stat != 'dividend':
            columns = ['date', key_stat]
        else:
            columns = ['Ex-Date', 'Record Date', 'Pay Date',
                       'Declared Date', 'Type', 'Amoun']
        last_page = self.get_last_page(company_name, key_stat)
        data = []
        df = pd.DataFrame(columns=columns, data=data)
        for i in tqdm(range(1, last_page + 1), desc=key_stat):
            data = self.get_data(key_stat=key_stat,
                                 company_name=company_name,  pageNum=i)
            d = pd.DataFrame(columns=columns, data=data)
            df = pd.concat([df, d], axis=0)
        return df

    def get(self, company_name, key_stat):
        self.main_data[key_stat] = self.get_key_stat(
            company_name, self.key_stats[key_stat])

    def get_all_key_stat(self, company_name, threadCount=26):
        count = int(len(self.key_stats) / threadCount) + 1
        keys = list(self.key_stats.keys())
        for i in tqdm(range(count), desc=company_name):
            th = []
            k = keys[i * threadCount: (i + 1) * threadCount]
            for key in k:
                t = threading.Thread(target=self.get, args=[company_name, key])
                t.start()
                th.append(t)
            for t in th:
                t.join()

    def get_data_fin(self, table, first_page=False):
        res = []
        for elem in table:
            try:
                name = elem.find('td')
                if name.text in self.skips:
                    continue
            except:
                continue
            if name.text in self.skips:
                continue
            r = []
            if first_page:
                r.append(name.text)
            r += [e.text.replace('\n', '').replace(' ', '')
                  for e in elem.findAll('td', {"class": "right"})]
            res.append(r)
        return res

    def get_columns_fin(self, table, first_page=False):
        if first_page:
            return [x.text for x in table[0].findAll('td')]
        return [x.text for x in table[0].findAll('td', {"class": "right"})]

    def get_fin(self, company_name, financials):
        url = 'https://ycharts.com/financials/{ticker}/{fin}/quarterly/{pageNum}'
        u = url.format(ticker=company_name, pageNum=1, fin=financials)
        r = self.s.get(u, headers=self.headers)
        if (r.status_code == 404):
            print('NOT FOUND\n')
            return
        soup = bs(r.text, features="lxml")
        table = soup.findAll('tr')
        df = pd.DataFrame(data=self.get_data_fin(table, True),
                          columns=self.get_columns_fin(table, True))
        idx = 2
        while True:
            u = url.format(ticker=company_name, pageNum=idx, fin=financials)
            r = self.s.get(u, headers=self.headers)
            if (r.status_code == 404):
                return df
            soup = bs(r.text, features="lxml")
            table = soup.findAll('tr')
            d = pd.DataFrame(data=self.get_data_fin(table),
                             columns=self.get_columns_fin(table))
            idx += 1
            df = pd.concat([df, d], axis=1)
        return df

    def get_all_financials(self, company_name):
        financials_stats = {
            'balance_sheet',
            'income_statement',
            'cash_flow_statement'
        }
        for key in financials_stats:
            self.main_data[key] = self.get_fin(company_name, key)
        self.save_fin(company_name)

    def save(self, company_name):
        try:
            os.mkdir('Data')
        except:
            pass
        try:
            os.mkdir('Data/stocks/' + company_name)
        except:
            pass
        for key in self.main_data.keys():
            if key not in self.financials_stats:
                self.main_data[key].to_csv('Data/stocks/' + company_name + '/key_stats/' +
                                      self.key_stats[key] + '.csv', index=False)
        print(company_name, 'показателей сохранено:', len(self.main_data.keys()))
        self.main_data = dict()
        
    
    def save_fin(self, company_name):
        try:
            os.mkdir('Data/stocks/' + company_name)
        except:
            pass
        try:
            os.mkdir('Data/stocks/' + company_name + '/financials_stats')
        except:
            pass
        for key in self.financials_stats:
            self.main_data[key].to_csv('Data/stocks/' + company_name + '/financials_stats/' +
                key + '.csv', index=False)
    
    def get_d(self, url, pageNum, index=False, price=False):
        u = url + '.json?endDate=01%2F23%2F2030&pageNum=' + str(pageNum) + '&startDate=12%2F12%2F1900'
        r = self.s.get(u, headers=self.headers)
        html = r.json()['data_table_html']
        soup = bs(html, features="lxml")
        if price == True or index == True:
            col1 = [x.text for x in soup.findAll("td", {"class": "col1"})]
            col2 = [x.text.split()[0] if len(x.text.split())
                else '' for x in soup.findAll("td", {"class": "col2"})]
            col3 = [x.text.split()[0] if len(x.text.split())
                else '' for x in soup.findAll("td", {"class": "col3"})]
            col4 = [x.text.split()[0] if len(x.text.split())
                else '' for x in soup.findAll("td", {"class": "col4"})]
            col5 = [x.text.split()[0] if len(x.text.split())
                else '' for x in soup.findAll("td", {"class": "col5"})]
            if price:
                col6 = [x.text.split()[0] if len(x.text.split())
                    else '' for x in soup.findAll("td", {"class": "col6"})]
                return [[col1[i], col2[i], col3[i], col4[i], col5[i], col6[i]] for i in range(len(col1))]
            return [[col1[i], col2[i], col3[i], col4[i], col5[i]] for i in range(len(col1))]
        col1 = [x.text for x in soup.findAll("td", {"class": "col1"})]
        col2 = [x.text.split()[0] if len(x.text.split())
                else '' for x in soup.findAll("td", {"class": "col2"})]
        data = [[col1[i], col2[i]] for i in range(len(col1))]
        return data
    
    def get_macro(self, url, index=False, price=False, save=False):
        u = url + '.json?endDate=01%2F23%2F2030&pageNum=1&startDate=12%2F12%2F1900'
        r = self.s.get(u, headers=self.headers)
        last_page = r.json()['last_page_num']
        data = []
        if index:
            columns = ['date', 'Open', 'High', 'Low', 'Close']
        else:
            columns = ['date', url.split('/')[-1]]
        if price:
            columns = ['date', 'Open', 'High', 'Low', 'Close', 'Volume']
        df = pd.DataFrame(columns=columns, data=data)
        for i in tqdm(range(1, last_page + 1), desc=url.split('/')[-1]):
            data = self.get_d(url,  i, index, price)
            d = pd.DataFrame(columns=columns, data=data)
            df = pd.concat([df, d], axis=0)
        if save:
            self.price_data[url.split('/')[-2]] = df
        return df
    
    def get_all_prices(self, company_list):
        first = company_list[:len(company_list)//2]
        second = company_list[len(company_list)//2:]
        th = []
        url = 'https://ycharts.com/companies/{company}/price'
        for company in first:
            t = threading.Thread(target=self.get_macro, args=[url.format(company=company), True, True, True])
            t.start()
            th.append(t)
        for t in th:
            t.join()
        for company in second:
            t = threading.Thread(target=self.get_macro, args=[url.format(company=company), True, True, True])
            t.start()
            th.append(t)
        for t in th:
            t.join()
            