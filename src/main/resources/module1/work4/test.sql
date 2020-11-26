use default;
load data inpath '/user_clicks/${click_date}/clicklog.dat' into table user_clicks partition(dt='${click_date}');
