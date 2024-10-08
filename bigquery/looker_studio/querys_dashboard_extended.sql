--Query FINAL
CREATE OR REPLACE TABLE
  factored-hackaton-2024.gdelt.extended_gkg_raw AS
SELECT
  *,  -- This will select all columns from the original gkg_raw table
  CASE 
    WHEN THEMES LIKE "%ECON_STOCKMARKET%" OR
         THEMES LIKE "%EPU_POLICY%" OR
         THEMES LIKE "%WB_1235_CENTRAL_BANKS%" OR
         THEMES LIKE "%UNEMPLOYMENT%" OR
         THEMES LIKE "%ECON_INFLATION%" OR
         THEMES LIKE "%ECON_BANKRUPTCY%" OR
         THEMES LIKE "%WB_1104_MACROECONOMIC_VULNERABILITY_AND_DEBT%" OR
         THEMES LIKE "%WB_2745_JOB_QUALITY_AND_LABOR_MARKET_PERFORMANCE%" OR
         THEMES LIKE "%POVERTY%" OR
         THEMES LIKE "%ECON_DEBT%" OR
         THEMES LIKE "%WB_471_ECONOMIC_GROWTH%" OR
         THEMES LIKE "%WB_318_FINANCIAL_ARCHITECTURE_AND_BANKING%" OR
         THEMES LIKE "%ECON_OILPRICE%" OR
         THEMES LIKE "%WB_625_HEALTH_ECONOMICS_AND_FINANCE%" OR
         THEMES LIKE "%ECON_FREETRADE%" THEN 'Economics'
    WHEN THEMES LIKE "%POLITICAL_TURMOIL%" OR
         THEMES LIKE "%REBELLION%" OR
         THEMES LIKE "%TRIAL%" OR
         THEMES LIKE "%TERROR%" OR
         THEMES LIKE "%MILITARY%" OR
         THEMES LIKE "%CORRUPTION%" OR
         THEMES LIKE "%PROTEST%" OR
         THEMES LIKE "%EXTREMISM%" OR
         THEMES LIKE "%REFUGEES%" OR
         THEMES LIKE "%SURVEILLANCE%" OR
         THEMES LIKE "%WB_2024_ANTI_CORRUPTION_AUTHORITIES%" OR
         THEMES LIKE "%EPU_CATS_NATIONAL_SECURITY%" OR
         THEMES LIKE "%DEMOCRACY%" OR
         THEMES LIKE "%WB_2019_ANTI_CORRUPTION_LEGISLATION%" OR
         THEMES LIKE "%WB_739_POLITICAL_VIOLENCE_AND_CIVIL_WAR%" THEN 'Political'
    WHEN THEMES LIKE "%WB_2443_RAPE_AND_SEXUAL_VIOLENCE%" OR
         THEMES LIKE "%IMMIGRATION%" OR
         THEMES LIKE "%SCIENCE%" OR
         THEMES LIKE "%EDUCATION%" OR
         THEMES LIKE "%ENV_CLIMATECHANGE%" OR
         THEMES LIKE "%ECON_ENTREPRENEURSHIP%" OR
         THEMES LIKE "%WB_2165_HEALTH_EMERGENCIES%" OR
         THEMES LIKE "%ECON_HOUSING_PRICES%" OR
         THEMES LIKE "%WB_2167_PANDEMICS%" OR
         THEMES LIKE "%WB_525_RENEWABLE_ENERG%" OR
         THEMES LIKE "%ECON_SUBSIDIES%" OR
         THEMES LIKE "%DISCRIMINATION_RACE_RACISM%" OR
         THEMES LIKE "%HEALTH_VACCINATION%" OR
         THEMES LIKE "%MEDIA_CENSORSHIP%" OR
         THEMES LIKE "%TAX_DISEASE%" OR
         THEMES LIKE "%TAX_DISEASE_CORONAVIRUS_INFECTIONS%" OR
         THEMES LIKE "%TAX_DISEASE_CORONAVIRUS%" THEN 'Social' 
    ELSE 'Other' END AS Main_Category,
  CASE
    -- Economic themes
    WHEN THEMES LIKE "%ECON_STOCKMARKET%" THEN "stock_market"
    WHEN THEMES LIKE "%EPU_POLICY%" THEN "incertidumbre_economica"
    WHEN THEMES LIKE "%WB_1235_CENTRAL_BANKS%" THEN "banco_mundial"
    WHEN THEMES LIKE "%UNEMPLOYMENT%" THEN "desempleo"
    WHEN THEMES LIKE "%ECON_INFLATION%" THEN "inflacion_economica"
    WHEN THEMES LIKE "%ECON_BANKRUPTCY%" THEN "quiebra_economica"
    WHEN THEMES LIKE "%WB_1104_MACROECONOMIC_VULNERABILITY_AND_DEBT%" THEN "macroeconomia_deuda_y_vulnerabilidad"
    WHEN THEMES LIKE "%WB_2745_JOB_QUALITY_AND_LABOR_MARKET_PERFORMANCE%" THEN "job_quality_&_labor_market_performance"
    WHEN THEMES LIKE "%POVERTY%" THEN "pobreza"
    WHEN THEMES LIKE "%ECON_DEBT" THEN "deuda_publica"
    WHEN THEMES LIKE "%WB_471_ECONOMIC_GROWTH%" THEN "crecimiento_economico"
    WHEN THEMES LIKE "%WB_318_FINANCIAL_ARCHITECTURE_AND_BANKING%" THEN "finanzas_y_bancos"
    WHEN THEMES LIKE "%ECON_OILPRICE%" THEN "precio_petroleo"
    WHEN THEMES LIKE "%WB_625_HEALTH_ECONOMICS_AND_FINANCE%" THEN "prosperidad_economica_y_finanzas"
    WHEN THEMES LIKE "%ECON_FREETRADE%" THEN "libre_comercio"
    -- Political themes
    WHEN THEMES LIKE "%POLITICAL_TURMOIL%" THEN "inestabilidad_politica"
    WHEN THEMES LIKE "%REBELLION%" THEN "rebelion"
    WHEN THEMES LIKE "%TRIAL%" THEN "juicio"
    WHEN THEMES LIKE "%TERROR%" THEN "terrorismo"
    WHEN THEMES LIKE "%MILITARY%" THEN "ejercito"
    WHEN THEMES LIKE "%CORRUPTION%" THEN "corrupcion"
    WHEN THEMES LIKE "%PROTEST%" THEN "protestas"
    WHEN THEMES LIKE "%EXTREMISM%" THEN "extremismo"
    WHEN THEMES LIKE "%REFUGEES%" THEN "refugiados"
    WHEN THEMES LIKE "%SURVEILLANCE%" THEN "vigilancia"
    WHEN THEMES LIKE "%WB_2024_ANTI_CORRUPTION_AUTHORITIES%" THEN "autoridades_anticorrupcion"
    WHEN THEMES LIKE "%EPU_CATS_NATIONAL_SECURITY%" THEN "seguridad_nacional"
    WHEN THEMES LIKE "%DEMOCRACY%" THEN "democracia"
    WHEN THEMES LIKE "%WB_2019_ANTI_CORRUPTION_LEGISLATION%" THEN "legislacion_anticorrupcion"
    WHEN THEMES LIKE "%WB_739_POLITICAL_VIOLENCE_AND_CIVIL_WAR%" THEN "violencia_politica_y_guerra_civil"
    -- Social themes
    WHEN THEMES LIKE "%WB_2443_RAPE_AND_SEXUAL_VIOLENCE%" THEN "agresion_sexual"
    WHEN THEMES LIKE "%IMMIGRATION%" THEN "inmigracion"
    WHEN THEMES LIKE "%SCIENCE%" THEN "ciencia"
    WHEN THEMES LIKE "%EDUCATION%" THEN "educacion"
    WHEN THEMES LIKE "%ENV_CLIMATECHANGE%" THEN "cambio_climatico"
    WHEN THEMES LIKE "%ECON_ENTREPRENEURSHIP%" THEN "emprendimiento"
    WHEN THEMES LIKE "%WB_2165_HEALTH_EMERGENCIES%" THEN "emergencia_sanitaria"
    WHEN THEMES LIKE "%ECON_HOUSING_PRICES%" THEN "precio_vivienda"
    WHEN THEMES LIKE "%WB_2167_PANDEMICS%" THEN "pandemia"
    WHEN THEMES LIKE "%WB_525_RENEWABLE_ENERG%" THEN "energias_renovables"
    WHEN THEMES LIKE "%ECON_SUBSIDIES%" THEN "subsidios"
    WHEN THEMES LIKE "%DISCRIMINATION_RACE_RACISM%" THEN "racismo"
    WHEN THEMES LIKE "%HEALTH_VACCINATION%" THEN "vacunas"
    WHEN THEMES LIKE "%MEDIA_CENSORSHIP%" THEN "censura_en_medios"
    WHEN THEMES LIKE "%TAX_DISEASE%" THEN "enfermedades_muy_infecciosas"
    WHEN THEMES LIKE "%TAX_DISEASE_CORONAVIRUS_INFECTIONS%" THEN "numero_de_contagios_covid"
    WHEN THEMES LIKE "%TAX_DISEASE_CORONAVIRUS%" THEN "fallecimiento_por_covid"
    ELSE "otros" END AS news_category
FROM
  factored-hackaton-2024.gdelt.gkg_raw;




-- select * from factored-hackaton-2024.gdelt.extended_gkg_raw;



