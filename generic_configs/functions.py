# Databricks notebook source
 def get_rules(tag):
  """
    loads data quality rules from a table
    :param tag: tag to match
    :return: dictionary of rules that matched the tag
  """
  rules = {}
  df = spark.read.table("rules")
  for row in df.filter(col("tag") == tag).collect():
    rules[row['name']] = row['constraint']
	quarantine_rules = "NOT({0})".format(" OR ".join(rules.values()))
  return quarantine_rules
