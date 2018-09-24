import sys
import snowflake.connector



def initConnection(user_name, user_password):
    username = user_name
    password = user_password
    accountName = 'hubspot.us-east-1'

    try:
        ctx = snowflake.connector.connect(
            user=username,
            password=password,
            account=accountName,
            authenticator='https://hubspot.okta.com',
        )

    except:
        print('Error - cannot connect to SnowFlake, exiting...')
        return None

    return ctx


def execute_query_with_result(ctx, sql_query):
    try:
        if ctx is not None:
            cs = ctx.cursor()
            #print("query to execute ", sql_query)
            sql_result = cs.execute(sql_query).fetchall()
            return sql_result
        else:
            print('Null connection to DB')

    except:
        print('Error running query')
        cs.close()
        exit(1)

    finally:
        cs.close()

def primary_key_check(user_name, user_password):
    ctx = initConnection(user_name, user_password)

    # Pull list of all fact tables from prod_db
    fact_table_list = execute_query_with_result(ctx,
        "select distinct table_name from prod_db.information_schema.columns where table_schema ilike 'fact_tables';")

    k = 0  # k is number to capture how manuy fact tables has primary key defined
    x = 0  # x is number to capture how many fact tables has non unique primary k

    for i in range(len(fact_table_list)):
        temp_fact_table = fact_table_list[i][0]
        temp_primary_key = ''
        has_primary_key = False
        try:
            temp_table_desc = execute_query_with_result(ctx,
                "desc table prod_db.fact_tables." + temp_fact_table)

            for j in range(len(temp_table_desc)):
                if temp_table_desc[j][5] == 'Y' and len(
                        temp_primary_key) == 0:  # find the primary key column and concatenate them
                    temp_primary_key = 'nvl(' + temp_table_desc[j][0] + ", '0')"
                    has_primary_key = True
                    k = k + 1
                elif temp_table_desc[j][5] == 'Y' and len(temp_primary_key) > 0:
                    temp_primary_key = temp_primary_key + "||" + 'nvl(' + temp_table_desc[j][0] + ", '0')"

            # print(has_primary_key,temp_fact_table, temp_primary_key)
            if has_primary_key:  # if table has primary key then test whether primary key is unique
                test_result = execute_query_with_result(ctx,
                    "select count(*) as row_cnt, count(distinct " + temp_primary_key + ") as primary_count from prod_db.fact_tables." + temp_fact_table)

                if test_result[0][0] != test_result[0][1]:
                    x = x + 1
                    print('fact_tables.' + temp_fact_table.lower() + ' primary key ' + temp_primary_key.lower().replace('||', ' + ') + ' is not unique, need further check')
        except:
            print('Final Check some issue')

    return None



if __name__ == "__main__":
    user_name = sys.argv[1]
    user_password = sys.argv[2]
    primary_key_check(user_name, user_password)




