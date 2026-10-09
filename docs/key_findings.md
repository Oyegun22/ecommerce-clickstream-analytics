# Key findings

October 2019, measured on the gold tables by `sql/analysis.sql`. Revenue is purchase price where `is_non_positive_price` is false: **229,933,212.63** from **742,773** purchases. Average order value is **309.56**. **347,118** of **3,022,290** users buy, a buyer rate of **0.114853**. Every figure below is a cell in `docs/analysis_results.md`.

Category is `dim_product`. Brand is the brand on the event. Hours are UTC. A cart is abandoned when the same session has no purchase of that same product. RFM recency is the number of days from a buyer's last purchase date to 2019-10-31.

## Insights

### 1. Most purchase sessions never cart

9,242,653 sessions contain a view. 571,907 of those also cart (rate 0.061877), and 291,500 view, cart, and purchase (rate 0.031539).

629,560 sessions contain a purchase. 337,699 of them have no cart event, which is a rate of 0.536405. Another 291,500 purchase with a view and a cart (0.463022), and 361 purchase with a cart and no view (0.000573).

The same split shows up inside categories and brands. Apparel has 26 cart events and 8,002 purchases. The lucente brand has 0 carts and 11,576 purchases. A cart-only view of checkout covers the sessions that cart and misses the larger purchase path.

### 2. Smartphones and Apple account for most of the revenue

`electronics` is 176,445,577.86, a revenue share of 0.767378, from 422,979 purchases at an average order value of 417.15. `electronics / smartphone` alone is 157,033,913.97, a share of 0.682954, at an average order value of 464.63.

Apple is 111,199,495.70, a share of 0.483617, average order value 778.39, and 0.037890 purchases per view. Samsung is 46,402,037.35, a share of 0.201807, average order value 268.41, and 0.035969 purchases per view. The two brands convert views at a similar rate. Apple's price is what separates the revenue.

Purchases priced 1,000 and above are 49,403 events, a purchase share of 0.066512, and 64,264,012.20 of revenue, a share of 0.279490. Purchases under 50 are 117,065 events, a purchase share of 0.157605, and 3,447,165.29 of revenue, a share of 0.014992.

The unknown category is second at level 1: 173,411 purchases, 22,922,165.08 of revenue, share 0.099691, average order value 132.18. Unknown brand is fourth: 58,210 purchases, 8,539,747.08, share 0.037140.

### 3. Browsing and buying peak at different UTC hours, and the month rises into mid-October

Event volume peaks at 16:00 UTC: 3,053,226 events, an event share of 0.071979, and 35,772 purchases. Purchase count peaks at 08:00 UTC: 55,195 purchases, a purchase share of 0.074309. Revenue peaks at 09:00 UTC: 17,551,669.73, a revenue share of 0.076334.

Wednesday is the strongest weekday by revenue, 37,064,086.24, a share of 0.161195. Tuesday has the most events, 6,797,348. The weekend rollup is 59,036,422.18, a share of 0.256755, across the Saturday and Sunday rows. The weekday rollup is 170,896,790.45, a share of 0.743245.

The highest day is 2019-10-16, with 31,393 purchases and revenue 9,747,164.72. The lowest day is 2019-10-02, with revenue 6,213,628.53. The 7-day average uses a full window from 2019-10-07. It peaks on 2019-10-19 at 8,708,230.73 and finishes on 2019-10-31 at 6,780,464.76.

### 4. Buyers with two or more purchase sessions produce most of the revenue

235,461 buyers have one purchase session. They are a buyer share of 0.678331 and a revenue share of 0.305044 (70,139,813.69). The other 111,657 buyers have two or more purchase sessions: a buyer share of 0.321669 and revenue of 159,793,398.94, a share of 0.694956. Their average monetary value is 1,431.11, against 297.88 for one-session buyers.

18,449 buyers with five or more purchase sessions are a buyer share of 0.053149 and a revenue share of 0.349755 (80,420,282.78).

RFM uses NTILE(5). Frequency scores 1, 2, and 3 each contain 69,424 buyers whose purchase-session minimum and maximum are both 1. Score 4 runs from 1 to 2 sessions. Score 5 runs from 2 to 152. Because those ties split one-session buyers across three scores, the segment label uses the raw session count, and the tile bounds are published next to it.

Champions (recency score at least 4, at least 3 purchase sessions, monetary score at least 4) are 27,453 buyers, a share of 0.079088, and 85,321,183.75 of revenue, a share of 0.371069. Average recency is 4.04 days and the maximum inside the segment is 10 days. Big spenders, one purchase session in monetary score 5, are 21,830 buyers and 27,709,328.89 of revenue, a share of 0.120510, with an average monetary value of 1,269.32.

Recency only ranks days inside this month. A need-attention buyer (30,297 buyers, revenue 29,633,223.38) last purchased 16 to 30 days before 31 October.

### 5. About half of carted products are abandoned, and the higher rate is on cheap carts

628,207 session-product pairs have a cart event. 323,267 of them have no purchase of that product in that session, an abandon rate of 0.514587. Abandoned cart value, the sum of the maximum cart price on those pairs, is 107,866,053.25. Purchased pairs sum to 107,147,653.43. The median cart price is 195.62 on abandoned pairs and 223.65 on purchased pairs.

The abandon rate is 0.622010 under 50 (abandoned value 1,213,538.33) and 0.506654 from 50 to 150. From 150 upward the rate sits between 0.493258 and 0.507742. The 1,000 and above band abandons at 0.503844 and holds 31,852,532.99 of abandoned cart value.

Electronics holds 89,266,881.40 of that abandoned value, a share of 0.827572, at a rate of 0.482476.

At session grain the picture is the same shape: 281,236 of 573,097 cart sessions have no purchase at all, a rate of 0.490730.

## Recommendations

1. **Report the direct purchase path beside cart abandonment.** 337,699 purchase sessions have no cart, including the apparel and lucente counts above. Cart abandonment is the right metric for the 628,207 carted pairs. It is a partial picture of how purchases happen on this site.

2. **Treat electronics / smartphone and Apple as the revenue core.** Those cuts are 157,033,913.97 and 111,199,495.70. Availability, price, and page quality there move the month. Samsung converts views at a close rate (0.035969 against Apple's 0.037890) and brings a lower average order value (268.41 against 778.39), so the two brands want different offers.

3. **Put retention against the repeat buyers and the one-session big spenders.** 111,657 buyers with two or more purchase sessions produce 159,793,398.94. Champions are a buyer share of 0.079088 and a revenue share of 0.371069. The 21,830 one-session big spenders add 27,709,328.89 at an average of 1,269.32. Recency in this file is position inside October, so a "need attention" flag means the last purchase fell 16 to 30 days before 31 October.

4. **Fill `category_code` on the unknown products before setting category targets.** That bucket is 173,411 purchases and 22,922,165.08. Unknown brand is smaller, 58,210 purchases and 8,539,747.08, and it is still the fourth brand by revenue. For cart recovery, the value sits in electronics (89,266,881.40) and in carts priced 1,000 and above (31,852,532.99). The higher abandon rate is under 50, where the abandoned value is 1,213,538.33.
