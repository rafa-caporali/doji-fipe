# Context (Doji / FIPE)

## Business
We connect people who want to sell used phones with businesses that want to buy them (and then resell).

- Supply acquisition: consumers trade in phones via partner physical stores.
- Demand / resale channels:
  - Primary: our direct-to-community store called "Intercept".
  - Fallback: monthly business bidders (B2B buyers) who purchase devices we cannot sell via Intercept.

## Monthly bidding (fallback channel)
Each month, multiple business bidders submit price sheets per phone model.

For each model, a bidder provides:
- A perfect-condition price (base price).
- A defect deduction table: percentage deductions per defect type (e.g., scratched screen, burn-in, microphone not working, scratched carcass, etc.).

Operationally:
- For each model, we select the best (highest) bidder as a baseline reference.
- Consumer buy prices (what we pay at trade-in) are derived from the selected bid:
  - Start from perfect-condition value.
  - Subtract defect deductions.
  - Apply a take rate so the consumer price is lower than what the bidder would pay.
- If a phone is not sold via Intercept after multiple attempts, it is sold to the selected bidder for its model.

## Intercept (direct-to-community store)
Intercept is a simple website connected to a WhatsApp community of buyers.

- Each offer is linked to a single physical phone, with photos and a fixed price.
- Offers are created daily, visible immediately, and expire when sold or at midnight of the creation day.
- Buying is released at specific times, typically in batches:
  - At the start of the day we announce time intervals.
  - Within each interval, the actual release time is arbitrary.
  - A WhatsApp community message is sent when a batch is released.
- This creates competition among buyers for batch offers.

Fallback behavior on Intercept:
- A phone not sold on day 1 can be offered on day 2 and day 3.
- Operators usually apply a markdown each day; currently this is judgment-based, typically targeting ~10% total variation, with no strict standard yet.
- If still unsold after the third offer, it goes to the monthly bidder.

## The "FIPE" table (the product we are building)
"FIPE" is an internal universal price reference table for used phones.

Goal:
- Build and maintain a FIPE table that becomes the global reference price standard for used phones.
- In the future, we plan to require customers to follow the FIPE pricing standard.

What we are tweaking/optimizing now:
- The FIPE reference price (the value in the FIPE table). This is the price we want to adjust using data.

Important nuance:
- We sell mostly to people who will resell (B2B-like behavior), so FIPE prices likely need to be lower than messy C2C used-phone prices. The correct relationship to external market prices is an open modeling question.

## Current optimization metric: "FIPE downgrade"
We currently optimize an internal KPI called "FIPE downgrade".

Per-phone quantities:
- `paid_amount`: what we actually got paid when the phone sold (e.g., on Intercept or fallback).
- `fipe_price`: FIPE reference price for that phone (based on model/variant/condition rules).
- `fipe_loss`: only captures underperformance vs FIPE (see definition below).
- `fipe_revenue`: projected revenue for that trade-in (normalization base).

Definition used today:
```
raw_diff_i = fipe_price_i - paid_amount_i
fipe_loss_i = raw_diff_i if raw_diff_i < 0 else 0
```
So `fipe_loss_i` is always <= 0 (or 0).

Per-phone downgrade (conceptual):
- `fipe_downgrade_i = abs(fipe_loss_i) / fipe_revenue_i`

Interval KPI (how we aggregate in time windows):
- `FIPE_downgrade_interval = (sum_i abs(fipe_loss_i)) / (sum_i fipe_revenue_i)`
  - where the sums are over all phones sold in the interval.

## Data we have (current sources)
1) Payment / sales data
- Time of payment
- Payment value
- Buyer identity (who bought)

2) Offer engagement data (Google Analytics events)
- Clicks on photos
- Clicks on offer modals
- Clicks on "buy now"
- Each event includes timestamps and user identity

3) Offer / inventory data
- Which phone was offered
- Offer price
- When the offer was created
- When the offer was released (batch drop)

4) External price references
- Scraped prices from big marketplaces and competitors

More data details (schema, grain, joins, definitions) to be documented separately as we refine.

## Project goal (algorithmic)
Develop an algorithm that updates/tweaks FIPE reference prices using:
- Intercept engagement signals (to understand demand pressure / willingness to pay)
- Conversion / realized payment outcomes (to measure achieved price vs FIPE)
- External price references (to anchor to the broader market)

The algorithm should reduce FIPE downgrade over time while keeping FIPE as a credible, enforceable reference for used-phone pricing.

## Non-goals (for now)
- Optimizing take rate policies (can vary, but not a focus at this stage).
- Standardizing operator day-2/day-3 markdown rules (currently judgment-based, open for future work).
