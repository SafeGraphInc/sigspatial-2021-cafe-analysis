# Databricks notebook source
# MAGIC %md # SafeGraph SIGSPATIAL 2021 Workshop
# MAGIC 
# MAGIC ## Understand Consumer Behavior With Verified Foot Traffic Data
# MAGIC 
# MAGIC **Background** Visitor and demographic aggregation data provide essential context on population behavior. How often a place of interest is visited, how long do visitors stay, where did they come from, and where are they going? The answers are invaluable in numerous industries. Building financial indicators, city and urban planning, public health indicators, or identifying your primary business competitors all require accurate, high quality population and POI data. 
# MAGIC 
# MAGIC **Objective** Our workshop’s objective is to provide professionals, researchers, and practitioners interested in deriving human movement patterns from location data. We use a sample of our Weekly and Monthly Patterns and Core Places products to perform market research on a potential new coffee shop location. We’ll address these concerns and more in building a market analysis proposal in real time. 
# MAGIC 
# MAGIC **Questions to Answer** 
# MAGIC - How far are customers willing to travel for coffee? 
# MAGIC - What location will receive the most visibility? 
# MAGIC - Where do most of the coffee customers come from? 

# COMMAND ----------

# MAGIC %md ## Notebook Setup

# COMMAND ----------

import pyspark.sql.functions as f
from pyspark.sql.types import MapType, StringType, IntegerType

import pandas as pd

# COMMAND ----------

# MAGIC %md ### Load in SafeGraph sample data from s3
# MAGIC The covers all coffee shops (`category_tag.contains("Coffee Shop")`) in Seattle (`county_FIPS.isin(["53033","53053","53061"]`), with multiple rows per POI corresponding to monthly foot traffic since the beginning of 2018 (1 month per row per POI).
# MAGIC 
# MAGIC Columns are SafeGraph Core, Geo, and Patterns pre-joined together with `placekey` as the join key.

# COMMAND ----------

sample_csv_path = 's3://safegraph-general-non-eng/jeff/misc/sigspatial/seattle_coffee_monthly_patterns/'
sample = (
  spark.read.option("header", "true").option("escape", "\"").csv(sample_csv_path)
  .withColumn('date_range_start', f.to_date(f.col('date_range_start')))
  .withColumn('date_range_end', f.to_date(f.col('date_range_end')))
  .withColumn('visitor_home_cbgs', f.from_json('visitor_home_cbgs', schema = MapType(StringType(), IntegerType())))
)

# COMMAND ----------

display(sample)

# COMMAND ----------

# MAGIC %md ## Exploratory Data Analysis and Visualization

# COMMAND ----------

#go through columns we'll use. Perform some aggregations and filters maybe. Some histograms.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Visualize the coffee shops

# COMMAND ----------

from pyspark.sql.window import Window
import geopandas as gpd
import folium

# COMMAND ----------

w = Window().partitionBy('placekey').orderBy(f.col('date_range_start').desc())

cafes_latest = (
  sample
  # as our data improves, addresses or geocodes for a given location may change over time
  # use a window function to keep only the most recent appearance of the given cafe
  .withColumn('row_num', f.row_number().over(w))
  .filter(f.col('row_num') == 1)
  # select the columns we need for mapping
  .select('placekey', 'location_name', 'brands', 'street_address', 'city', 'region', 'postal_code', 'latitude', 'longitude', 'open_hours')
)

# COMMAND ----------

# create a geopandas geodataframe
cafes_gdf = cafes_latest.toPandas()
cafes_gdf = gpd.GeoDataFrame(cafes_gdf, geometry = gpd.points_from_xy(cafes_gdf['longitude'], cafes_gdf['latitude']), crs = 'EPSG:4326')

# COMMAND ----------

def map_cafes(gdf):
  
  # map bounds
  sw = [gdf.unary_union.bounds[1], gdf.unary_union.bounds[0]]
  ne = [gdf.unary_union.bounds[3], gdf.unary_union.bounds[2]]
  folium_bounds = [sw, ne]
  
  # map
  x = gdf.centroid.x[0]
  y = gdf.centroid.y[0]
  
  map_ = folium.Map(
    location = [y, x],
    tiles = "OpenStreetMap"
  )
  
  for i, point in gdf.iterrows():
    
    tooltip = f"placekey: {point['placekey']}<br>location_name: {point['location_name']}<br>brands: {point['brands']}<br>street_address: {point['street_address']}<br>city: {point['city']}<br>region: {point['region']}<br>postal_code: {point['postal_code']}<br>open_hours: {point['open_hours']}"
    
    folium.Circle(
      [point['geometry'].y, point['geometry'].x],
      radius = 40,
      fill_color = 'blue',
      color = 'blue',
      fill_opacity = 1,
      tooltip = tooltip
    ).add_to(map_)

  map_.fit_bounds(folium_bounds) 
  
  return map_

# COMMAND ----------

map_ = map_cafes(cafes_gdf)
map_

# COMMAND ----------

# MAGIC %md ## Analysis

# COMMAND ----------

# MAGIC %md ### How far are people willing to travel for coffee?

# COMMAND ----------

# the `distance_from_home` column tells us the median distance (as the crow flies), in meters, between the coffee shop and the visitors' homes 
# which coffee shop's visitors had the highest average median distance traveled since Jan 2018?

# outlier values in this column distort the histogram
# these outliers are likely due to a combination of (1) coffee shops in downtown areas that receive high numbers of out-of-town visitors and (2) quirks in the underlying GPS data
furthest_traveled = (
  sample
  .groupBy('placekey', 'location_name')
  .agg(f.mean('distance_from_home').alias('avg_median_dist_from_home'))
  .orderBy('avg_median_dist_from_home', ascending = False)
)

display(furthest_traveled)

# COMMAND ----------

# most coffee shops' visitors' homes are <10km away
display(furthest_traveled.filter(f.col('avg_median_dist_from_home') < 25000))

# COMMAND ----------

# but `distance_from_home` takes into account ALL visitors to the coffee shop, which as we saw above can distort values. When selecting a site for a coffee shop, we likely care more about how far visitors traveled from within Seattle
# we can compute this using Sedona
import sedona
from sedona.register import SedonaRegistrator
from sedona.utils import SedonaKryoRegistrator, KryoSerializer

SedonaRegistrator.registerAll(spark)

# COMMAND ----------

# load the census block groups for Washington State
# filter it down to our three counties of interest in Seattle 
WA_cbgs = (
  spark.read.option('header', 'true').option('escape', "\"").csv('s3://safegraph-general-non-eng/jeff/misc/sigspatial/WA_CBG.csv')
  .filter(f.col('GEOID').rlike('^(53033|53053|53061)'))
)
display(WA_cbgs)

# COMMAND ----------

# transform the geometry column into a Geometry-type
WA_cbgs = (
  WA_cbgs
  .withColumn('cbg_geometry', f.expr("ST_GeomFromWkt(geometry)"))
  # we'll just use the CBG centroid
  .withColumn('cbg_geometry', f.expr("ST_Centroid(cbg_geometry)"))
  # since we'll be doing a distance calculation, let's also use a projected CRS - epsg:3857
  .withColumn('cbg_geometry', f.expr("ST_Transform(ST_FlipCoordinates(cbg_geometry), 'epsg:4326','epsg:3857', false)")) # ST_FlipCoordinates() necessary due to this bug: https://issues.apache.org/jira/browse/SEDONA-39
  .withColumnRenamed('GEOID', 'cbg')
  .withColumnRenamed('geometry', 'cbg_polygon_geometry')
)

# COMMAND ----------

display(WA_cbgs)

# COMMAND ----------

# Next let's prep our sample data
sample_seattle_visitors = (
  sample
  .select('placekey', f.explode('visitor_home_cbgs'))
  .withColumnRenamed('key', 'cbg')
  .withColumnRenamed('value', 'visitors')
#   # filter out CBGs with low visitor counts
  .filter(f.col('visitors') > 4)
  # filter down to only the visitors from Seattle CBGs
  .filter(f.col('cbg').rlike('^(53033|53053|53061)'))
  # aggregate up all the visitors over time from each CBG to each Cafe
  .groupBy('placekey', 'cbg')
  .agg(
    f.sum('visitors').alias('visitors')
  )
  # join back with most up-to-date POI information
  .join(
    cafes_latest.select('placekey', 'latitude', 'longitude'),
    'placekey'
  )
  # transform geometry column
  .withColumn('cafe_geometry', f.expr("ST_Point(CAST(longitude AS Decimal(24, 20)), CAST(latitude AS Decimal(24, 20)))"))
  .withColumn('cafe_geometry', f.expr("ST_Transform(ST_FlipCoordinates(cafe_geometry), 'epsg:4326','epsg:3857', false)"))
  # join with CBG geometries
  .join(WA_cbgs, 'cbg')
)

# COMMAND ----------

display(sample_seattle_visitors)

# COMMAND ----------

distance_traveled_SEA = (
  sample_seattle_visitors
  # calculate the distance from home in meters
  .withColumn('distance_from_home', f.expr("ST_Distance(cafe_geometry, cbg_geometry)"))
)
distance_traveled_SEA.createOrReplaceTempView('distance_traveled_SEA')

# COMMAND ----------

q = '''
SELECT *
FROM (
  SELECT DISTINCT 
    placekey, 
    cbg,
    visitors,
    distance_from_home,
    posexplode(split(repeat(",", visitors), ","))
    FROM distance_traveled_SEA
)
WHERE pos > 0
'''

weighted_median_tmp = spark.sql(q)

# COMMAND ----------

grp_window = Window.partitionBy('placekey')
median_percentile = f.expr('percentile_approx(distance_from_home, 0.5)')

# COMMAND ----------

display(weighted_median_tmp)

# COMMAND ----------

median_dist_traveled_SEA = (
  weighted_median_tmp
  .groupBy('placekey')
  .agg(median_percentile.alias('median_dist_traveled_SEA'))
)

# COMMAND ----------

display(
  median_dist_traveled_SEA.filter(f.col('median_dist_traveled_SEA') < 25000)
)

# COMMAND ----------

total_visits = (
  sample
  .groupBy('placekey')
  .agg(
    f.sum('raw_visit_counts').alias('total_visits'),
    f.sum('raw_visitor_counts').alias('total_visitors')
  )
)

# COMMAND ----------

distance_traveled_final = (
  furthest_traveled
  .join(median_dist_traveled_SEA, 'placekey')
  .join(cafes_latest, ['placekey', 'location_name'])
  .withColumn('distance_traveled_diff', f.col('avg_median_dist_from_home') - f.col('median_dist_traveled_SEA'))
  # keep only the cafes with a meaningful sample - at least 1000 visits since 2018
  .join(total_visits, 'placekey')
  .filter(f.col('total_visits') > 1000)
  .orderBy('distance_traveled_diff', ascending = False)
)

# COMMAND ----------

display(distance_traveled_final)

# COMMAND ----------

distance_traveled_final.select('placekey').distinct().count()

# COMMAND ----------

most_tourists = distance_traveled_final.limit(500).withColumn('visitor_type', f.lit('tourist'))
most_locals = distance_traveled_final.orderBy('distance_traveled_diff').limit(500).withColumn('visitor_type', f.lit('local'))

visitor_type = most_tourists.unionByName(most_locals)

# create a geopandas geodataframe
visitor_type_gdf = visitor_type.toPandas()
visitor_type_gdf = gpd.GeoDataFrame(visitor_type_gdf, geometry = gpd.points_from_xy(visitor_type_gdf['longitude'], visitor_type_gdf['latitude']), crs = 'EPSG:4326')

# COMMAND ----------

def map_cafe_visitor_type(gdf):
  
  # map bounds
  sw = [gdf.unary_union.bounds[1], gdf.unary_union.bounds[0]]
  ne = [gdf.unary_union.bounds[3], gdf.unary_union.bounds[2]]
  folium_bounds = [sw, ne]
  
  # map
  x = gdf.centroid.x[0]
  y = gdf.centroid.y[0]
  
  map_ = folium.Map(
    location = [y, x],
    tiles = "OpenStreetMap"
  )
  
  for i, point in gdf.iterrows():
    
    tooltip = f"placekey: {point['placekey']}<br>location_name: {point['location_name']}<br>brands: {point['brands']}<br>street_address: {point['street_address']}<br>city: {point['city']}<br>region: {point['region']}<br>postal_code: {point['postal_code']}<br>visitor_type: {point['visitor_type']}<br>avg_median_dist_from_home: {point['avg_median_dist_from_home']}"
    
    folium.Circle(
      [point['geometry'].y, point['geometry'].x],
      radius = 40,
      fill_color = 'blue' if point['visitor_type'] == 'tourist' else 'red',
      color = 'blue' if point['visitor_type'] == 'tourist' else 'red',
      fill_opacity = 1,
      tooltip = tooltip
    ).add_to(map_)

  map_.fit_bounds(folium_bounds) 
  
  return map_

# COMMAND ----------

# blue = touristy, red = locals
map_cafe_visitor_type(visitor_type_gdf)

# COMMAND ----------

# MAGIC %md ### What location will receive the most visibility?
# MAGIC 
# MAGIC In other words, in what neighborhood do coffee shops get the most visits generally?

# COMMAND ----------

WA_neighbs = (
  spark.read.option('header', 'true').option('escape', "\"").csv('s3://safegraph-general-non-eng/jeff/misc/sigspatial/Seattle_neighborhoods.csv')
  # transform the geometry column into a Geometry-type
  .withColumn('geometry', f.expr("ST_GeomFromWkt(geometry)"))
)
WA_neighbs.createOrReplaceTempView('WA_neighbs')

display(WA_neighbs)

# COMMAND ----------

cafes_geo = cafes_latest.withColumn('cafe_geometry', f.expr("ST_Point(CAST(longitude AS Decimal(24,20)), CAST(latitude AS Decimal(24,20)))")).select('placekey', 'cafe_geometry')
cafes_geo.createOrReplaceTempView('cafes_geo')

# COMMAND ----------

# perform a spatial join
q = '''
SELECT cafes_geo.placekey, WA_neighbs.S_HOOD as neighborhood, WA_neighbs.geometry
FROM WA_neighbs, cafes_geo
WHERE ST_Intersects(WA_neighbs.geometry, cafes_geo.cafe_geometry)
'''

cafe_neighb_join = spark.sql(q)

# COMMAND ----------

display(cafe_neighb_join)

# COMMAND ----------

# add the visit and visitor counts and aggregate up to the neighborhood
neighborhood_agg = (
  cafe_neighb_join
  .join(total_visits, 'placekey')
  .groupBy('neighborhood', f.col('geometry').cast('string').alias('geometry'))
  .agg(
    f.sum('total_visits').alias('total_visits'),
    f.sum('total_visitors').alias('total_visitors')
  )
)

# COMMAND ----------

neighbs_gdf = (
  neighborhood_agg
  .toPandas()
)
neighbs_gdf = gpd.GeoDataFrame(neighbs_gdf, geometry = gpd.GeoSeries.from_wkt(neighbs_gdf['geometry']), crs = 'EPSG:4326')

# COMMAND ----------

def map_neighbs(gdf):
  
  # map bounds
  sw = [gdf.unary_union.bounds[1], gdf.unary_union.bounds[0]]
  ne = [gdf.unary_union.bounds[3], gdf.unary_union.bounds[2]]
  folium_bounds = [sw, ne]
  
  # map
  x = gdf.centroid.x[0]
  y = gdf.centroid.y[0]
  
  map_ = folium.Map(
    location = [y, x],
    tiles = "OpenStreetMap"
  )
    
  gdf['percentile'] = pd.qcut(gdf['total_visits'], 100, labels=False) / 100
  
  folium.GeoJson(
      gdf[['neighborhood', 'total_visits', 'total_visitors', 'percentile', 'geometry']],
      style_function = lambda x: {
        'weight':0,
        'color':'blue',
        'fillOpacity': x['properties']['percentile']
      },
    tooltip = folium.features.GeoJsonTooltip(
        fields = ['neighborhood', 'total_visits', 'total_visitors', 'percentile']
      )
    ).add_to(map_)

  map_.fit_bounds(folium_bounds) 
  
  return map_

# COMMAND ----------

map_neighbs(neighbs_gdf)

# COMMAND ----------

# MAGIC %md ### What home location has the most coffee-shop-goers?

# COMMAND ----------

home_loc_most_cafe_visitors = (
  sample
  .select(f.explode('visitor_home_cbgs'))
  .withColumnRenamed('key', 'cbg')
  .withColumnRenamed('value', 'visitors')
  .groupBy('cbg')
  .agg(f.sum('visitors').alias('visitors'))
  .orderBy('visitors', ascending = False)
)

# COMMAND ----------

# 40k visitors to Seattle coffee shops since Jan 2018 originated from CBG `530330082001`, just under twice as many as the next CBG.
display(home_loc_most_cafe_visitors)

# COMMAND ----------

from shapely import wkt

# COMMAND ----------

# Map of the top 1000 CBGs in terms of visitors' origins
home_loc_gdf = (
  home_loc_most_cafe_visitors
  .limit(1000)
  .join(
    WA_cbgs.select('cbg', 'cbg_polygon_geometry'),
    'cbg'
  )
  .toPandas()
)
home_loc_gdf = gpd.GeoDataFrame(home_loc_gdf, geometry = gpd.GeoSeries.from_wkt(home_loc_gdf['cbg_polygon_geometry']), crs = 'EPSG:4326')

# COMMAND ----------

def map_cbgs(gdf):
  
  # map bounds
  sw = [gdf.unary_union.bounds[1], gdf.unary_union.bounds[0]]
  ne = [gdf.unary_union.bounds[3], gdf.unary_union.bounds[2]]
  folium_bounds = [sw, ne]
  
  # map
  x = gdf.centroid.x[0]
  y = gdf.centroid.y[0]
  
  map_ = folium.Map(
    location = [y, x],
    tiles = "OpenStreetMap"
  )
  
  gdf['quantile'] = pd.qcut(gdf['visitors'], 100, labels=False) / 100
  
  folium.GeoJson(
      gdf[['cbg', 'visitors', 'geometry', 'quantile']],
      style_function = lambda x: {
        'weight':0,
        'color':'blue',
        'fillOpacity': x['properties']['quantile']
      },
    tooltip = folium.features.GeoJsonTooltip(
        fields = ['cbg', 'visitors', 'quantile']
      )
    ).add_to(map_)

  map_.fit_bounds(folium_bounds) 
  
  return map_

# COMMAND ----------

# top 1000 home_cbgs by number of people who visited coffee shops in Seattle
map_cbgs(home_loc_gdf)

# COMMAND ----------

# MAGIC %md ## Conclusions

# COMMAND ----------

# MAGIC %md ## Next Steps
# MAGIC 
# MAGIC Now you try!
# MAGIC 
# MAGIC 1. Replace the data loading step with this data path, which is similar data for Houston TX.
# MAGIC 2. Repeat the same analysis and answer the same questions!
