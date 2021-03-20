from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes
from pyflink.table.udf import udf

#Creating Stream Execution environment ---

env = StreamExecutionEnvironment.get_execution_environment()

env.set_parallelism(1)



#Creating Stream Table environment ---

tbl_env = StreamTableEnvironment.create(env)


#Creating sink/source ---

tbl_env.execute_sql(
            """
            CREATE TABLE source (
                territory VARCHAR, name VARCHAR,
                daysSinceLastCase INT, transmissionType INT,
                newDeaths INT, deaths INT, cases INT, newCases INT,
                reportDate VARCHAR, reportNumber INT
            )

            WITH (
                'connector'='filesystem',
                'path'='/home/tom/Documents/pythonfiles/flink_stuff/git_project/input',
                'format'='csv'
            )
            """
        )

#Loading data from source and transforming it ---

df = tbl_env.from_path("source")

df = df.drop_columns(df.territory, df.daysSinceLastCase, df.transmissionType, df.newCases, df.newDeaths, df.reportDate, df.reportNumber)

@udf(result_type = DataTypes.STRING())
def find_regions(country):
    africa = ["South Africa", "Nigeria", "Ethiopia",
              "Madagascar", "Ghana",
              'Democratic Republic of the Congo', 'Algeria', 'Kenya', 'Gabon', 'Senegal'
              'Zimbabwe', 'Cote dIvoire', 'Cameroon', 'Central African Republic', 'Congo'
              'Mauritania', 'Guinea', 'Zambia', 'Mozambique', 'South Sudan', 'Namibia'
              'Equatorial Guinea', 'Benin', 'Malawi', 'Liberia', 'Mali', 'Angola'
              'Cabo Verde', 'Eswatini', 'Rwanda', 'Chad', 'Sierra Leone', 'Botswana'
               'Burkina Faso', 'Comoros', 'Gambia', 'Uganda', 'Niger', "Eritrea",
               "Lesotho", "Togo", "Mauritius", "United Republic of Tanzania", "Burundi"]

    south_america = ['Mayotte', 'Ecuador', 'Peru', 'El Salvador',
                     'Honduras', 'Chile', 'Colombia', 'Suriname', 'Guatemala'
                    'Dominican Republic', 'Bolivia', 'Trinidad and Tobago', 'Haiti'
                    'Saint Lucia', 'Venezuela', 'Costa Rica', 'Paraguay', 'Bahamas', 'Aruba'
                    'Barbados', 'Cuba', 'Nicaragua', 'Uruguay', 'Belize', 'Dominica', 'Guyana'
                    'Jamaica', 'Sint Maarten', 'Guadeloupe', 'Grenada', 'Antigua and Barbuda'
                    'Falkland Islands', 'Saint Vincent and the Grenadines', 'Bermuda'
                    'French Guiana', 'Puerto Rico', 'United States Virgin Islands', 'Anguilla']

    europe = ['Russian Federation', 'Italy', 'Switzerland', 'France', 'Ukraine' 
            'The United Kingdom', 'Belgium', 'Ireland',
            'Germany', 'Romania', 'Sweden', 'Bulgaria', 'Portugal',
            'Belarus', 'Luxembourg', 'Poland', 'Czechia',
            'Serbia', 'Montenegro', 'Kyrgyzstan', 'Austria', 'Norway', 'Iceland', 'Denmark',
            'Republic of Moldova', 'Bosnia and Herzegovina', 'Finland', 'Andorra',
            'North Macedonia', 'Greece', 'Hungary', 'Slovenia', 'Tajikistan', 'Albania',
            'Faroe Islands', 'Estonia', 'Croatia', 'Cyprus', 'India', 'Slovakia', 'Malta']


    if country in africa:
        return "Africa"
    elif country in south_america:
        return "South America"
    elif country in europe:
        return "Europe"
    else:
        return "Other"



df = df.select(df.name, find_regions(df.name).alias('region'), df.deaths)



df = df.group_by(df.region).select(df.region, df.deaths.sum.alias("sum_of_deaths"))

df = df.to_pandas()

#Writing data

df.to_csv("/home/tom/Documents/pythonfiles/flink_stuff/git_project/output/covid_19.csv")
