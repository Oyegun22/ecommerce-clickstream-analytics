# LinkedIn

Figures are cells from `docs/analysis_results.md`. Revenue is purchase price where `is_non_positive_price` is false. Hours are UTC. RFM recency is days before 2019-10-31.

Replace `YOUR_USER` and `YOUR_STREAMLIT_APP_URL` before posting.

## Main post

337,699 purchase sessions never touched the cart.

I modeled the public REES46 October 2019 clickstream so I could say where the revenue sits, and which funnel would miss it. The file is Michael Kechinov’s [eCommerce behavior data from multi category store](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store), and the October archive is [2019-Oct.csv.gz](https://data.rees46.com/datasets/marketplace/2019-Oct.csv.gz). The raw file has 42,448,764 events. The gold fact keeps 42,418,542 after exact-duplicate and null-session removal, and positive-price purchases sum to 229,933,212.63.

The warehouse is bronze Parquet, a cleaned silver table, and a gold star schema. DuckDB runs the metric SQL on the gold files. The Streamlit app reads 354,128 bytes of aggregates.

Three measurements from that model:

1. 629,560 sessions contain a purchase, and 337,699 of them have no cart event, a rate of 0.536405. Apparel shows the same split in miniature: 26 cart events and 8,002 purchases.
2. `electronics / smartphone` is 157,033,913.97, a revenue share of 0.682954. Apple is 111,199,495.70 at an average order value of 778.39. Samsung is 46,402,037.35 at 268.41. Purchases per view are 0.037890 and 0.035969.
3. 111,657 buyers with two or more purchase sessions are a buyer share of 0.321669 and produce 159,793,398.94, a revenue share of 0.694956.

Stack: PySpark 3.5.3, Parquet, DuckDB 1.5.6, Streamlit, and Plotly. The hour peaks stay in UTC because the file has no store time zone.

Code: https://github.com/YOUR_USER/ecommerce-clickstream-analytics
Dashboard: https://YOUR_STREAMLIT_APP_URL

## Shorter variant A

337,699 of 629,560 purchase sessions in the REES46 October 2019 log have no cart event (rate 0.536405).

I built a PySpark medallion model and a DuckDB metric layer on 42,418,542 cleaned events. Positive-price revenue is 229,933,212.63. A cart-only funnel covers the sessions that cart and leaves out the larger purchase path. Apparel has 26 carts and 8,002 purchases.

Code: https://github.com/YOUR_USER/ecommerce-clickstream-analytics
Dashboard: https://YOUR_STREAMLIT_APP_URL

## Shorter variant B

`electronics / smartphone` is 157,033,913.97 of REES46 October 2019 revenue, a share of 0.682954.

Apple is 111,199,495.70 at an average order value of 778.39. Samsung is 46,402,037.35 at 268.41, with purchases per view of 0.035969 against Apple's 0.037890. The price is what separates the revenue. The model is PySpark, Parquet, and DuckDB, with a Streamlit app on the aggregates.

Code: https://github.com/YOUR_USER/ecommerce-clickstream-analytics
Dashboard: https://YOUR_STREAMLIT_APP_URL

## Hashtags

#DataEngineering #AnalyticsEngineering #Ecommerce #PySpark #DuckDB #Streamlit #Portfolio

## Projects entry

Title: REES46 October 2019 clickstream

- Modeled 42,418,542 cleaned events in a PySpark medallion warehouse. Positive-price purchase revenue is 229,933,212.63 from 742,773 purchases.
- Measured the purchase path in DuckDB: 337,699 of 629,560 purchase sessions have no cart, and `electronics / smartphone` is 157,033,913.97 of revenue.
- Published a Streamlit dashboard on 354,128 bytes of aggregates, with UTC dates, category filters, and a full-month RFM table.
