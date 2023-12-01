import pandas as pd
import plotly.graph_objs as go
# from viz_helper.layout import *
import psycopg2
import pandas as pd

def connect():
    """ Connect to the PostgreSQL database server """
    return psycopg2.connect(
                host="postgres",
                port="5432",
                database="test",
                user="test",
                password="test")

def generate_plotly_viz(df, viz_type, viz_name,yaxis=None):
    if viz_type=='bar':
        fig = go.Figure(
                data=[go.Bar(x=df.page_num, y=df[yaxis])],
                layout=go.Layout(
                    title=go.layout.Title(text=viz_name)
                )
            )

    if viz_type=='table':
        fig = go.Figure(data=[go.Table(
                        header=dict(values=list(df.columns),
                                    fill_color='paleturquoise',
                                    align='left'),
                        cells=dict(values=[df.user_id, df.sess,df.start_time,df.end_time,df.start_page,df.end_page,df.diff_time_min,df.steps],
                                fill_color='lavender',
                                align='left'))
                    ],
                layout=go.Layout(
                    title=go.layout.Title(text=viz_name)
                ))
    return fig

def generate_grouped(df1,df2,df3,viz_name):
    fig = go.Figure(data=[
    go.Bar(name='Most Visited', x=df1.page_num, y=df1.visited),
    go.Bar(name='Most Popular', x=df2.page_num, y=df2.popularity),
    go.Bar(name='Most Transcations', x=df3.page_num, y=df3.transactions)
    ])
    # Change the bar mode
    fig.update_layout(barmode='group',title=go.layout.Title(text=viz_name))
    return fig

def generate_plotly_viz_indicator(indicator):
    return go.Figure(go.Indicator(
                mode = "gauge+number",
                value = indicator,
                domain = {'x': [0, 1], 'y': [0, 1]},
                title = {'text': "User Spend Time"}))


def get_user_spend():
    """ query data from the table table """
    try:
        conn=connect()
        cur = conn.cursor()
        cur.execute('''select avg("public"."time_spend_per_ses".date_diff_min) as time_spend_minutes from "public"."time_spend_per_ses"''')
        print("The number of parts: ", cur.rowcount)
        tuples_list = cur.fetchall()
        rs = tuples_list[0]
        return round(rs[0],2)

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def get_visited_page():
    try:
        conn=connect()
        cur = conn.cursor()
        cur.execute('''select "public"."user_browsing".page as page_num,COUNT( DISTINCT "public"."user_browsing".user_id) as visited from user_browsing group by "public"."user_browsing".page order by  SUBSTRING(page FROM '([0-9]+)')::INT ASC, page ''')
        print("The number of parts: ", cur.rowcount)
        tuples_list = cur.fetchall()
        df = pd.DataFrame(tuples_list, columns=['page_num', 'visited'])

        return df

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def get_most_popular_page():
    
    try:
        conn=connect()
        cur = conn.cursor()
        cur.execute('''select "public"."user_browsing".page as page_num,COUNT( DISTINCT "public"."user_browsing".session_id) as popularity from user_browsing group by "public"."user_browsing".page order by  SUBSTRING(page FROM '([0-9]+)')::INT ASC, page''')
        print("The number of parts: ", cur.rowcount)
        tuples_list = cur.fetchall()
        df = pd.DataFrame(tuples_list, columns=['page_num', 'popularity'])

        return df

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def get_transactions():
    """ query data from the visited_pages table """
    try:
        conn=connect()
        cur = conn.cursor()
        cur.execute('''select page as page_num,count(distinct "public"."end_transactions".session_id ) as transactions from end_transactions group by page order by  SUBSTRING(page FROM '([0-9]+)')::INT ASC, page''')
        print("The number of parts: ", cur.rowcount)
        tuples_list = cur.fetchall()
        df = pd.DataFrame(tuples_list, columns=['page_num', 'transactions'])
        cur.close()
        return df

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def get_table():
    """ query data from with steps table """
    try:
        conn=connect()
        cur = conn.cursor()
        cur.execute('''
        with cte as (select *, rank() over(partition by user_id,session_id,transaction_timestamp order by browse_timestamp) as b_order from user_total_journey ),

        cte1 as (select *,lag(cte.browse_timestamp) over (partition by cte.user_id,cte.session_id,cte.transaction_timestamp order by cte.b_order)  as lag1 from cte),

        cte2 as (select *,DATE_PART('Minute',cte1.browse_timestamp-cte1.lag1) as dp from cte1),

        c2 as (select count(cte2.page) as steps,cte2.user_id,cte2.session_id,avg(cte2.dp) from cte2 group by cte2.user_id,cte2.session_id),
        c3 as (select user_journey_to_buy_visited.user_id as user_id , user_journey_to_buy_visited.session_id as sess, 
        user_journey_to_buy_visited.browse_timestamp as start_time, 
        user_journey_to_buy_visited.transaction_timestamp as end_time,
        user_journey_to_buy_visited.page as start_page,
        end_transactions.page as end_page,
        -- DATE_PART('Minute',user_journey_to_buy_visited.transaction_timestamp-user_journey_to_buy_visited.browse_timestamp) as diff_time_min
        ((DATE_PART('Day', user_journey_to_buy_visited.transaction_timestamp::TIMESTAMP - user_journey_to_buy_visited.browse_timestamp::TIMESTAMP) * 24 +
        DATE_PART('Hour', user_journey_to_buy_visited.transaction_timestamp::TIMESTAMP - user_journey_to_buy_visited.browse_timestamp::TIMESTAMP)) * 60 +
        DATE_PART('Minute', user_journey_to_buy_visited.transaction_timestamp::TIMESTAMP - user_journey_to_buy_visited.browse_timestamp::TIMESTAMP)) as diff_time_min
        
        from user_journey_to_buy_visited left join end_transactions on user_journey_to_buy_visited.user_id =end_transactions.user_id and user_journey_to_buy_visited.session_id =end_transactions.session_id)

        select c3.*,c2.steps from c3 left join  c2  on c3.user_id = c2.user_id and c3.sess=c2.session_id where c3.end_page is not null order by diff_time_min desc;  
        ''')
        print("The number of parts: ", cur.rowcount)
        tuples_list = cur.fetchall()
        df = pd.DataFrame(tuples_list, columns=['user_id', 'sess','start_time','end_time','start_page','end_page','diff_time_min','steps'])
        cur.close()
        return df

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

