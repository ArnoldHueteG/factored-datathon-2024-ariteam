SELECT * FROM factored-hackaton-2024.gdelt.filtered_news;
SELECT news_category, count(1) FROM `factored-hackaton-2024.gdelt.filtered_news` group by news_category order by 2 desc;
SELECT * FROM factored-hackaton-2024.gdelt.gkg_raw;
