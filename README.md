<div align="center">
  <img src="https://media1.giphy.com/media/v1.Y2lkPTc5MGI3NjExemFvM3g5aDhxa2twdXF6OGR6N2hkc2R4Z3V5YnJyejU3NnptaTMyciZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/Yj2nHhbGsNQSrGyvI7/giphy.gif" width="300" />

  ## 🚧 Work In Progress 🚧
  
  **Note: This project is under development. Expect breaking changes, incomplete documentation, and the occasional digital fire.**
</div>

---

# FPL-Optima ⚽
### *A Hybrid Machine Learning & Linear Programming Solver*

Hello!! This is my first project where I am parallely working on Github. Everybody loves football and it has been a big part of my life. When I moved to the UK I found myself actively watching the PL and start transferring in and out players on the regular as one does based on how they performed the current form, chance of an assist or a goal, etc. So I sat there thinking if it's possible for us to get the best possible 11 for the upcoming weeks. Well there you go that is the aim of this project.

## Project Roadmap

### Phase 1: Data Infrastructure & Orchestration (Current)

- [x] **API Integration:** Establish pipelines for FPL bootstrap-static and fixtures endpoints.

- [x] **Active Players Filtering**: Implement logic to filter out transferred/unavailable and those with 100% chance of playing.

- [x] **Sample Size Filtering:** Apply a minutes >= 270 threshold to ensure statistical significance for player metrics.

- [x] **Scribbler Validation (Whiteboard Phase):** Prototypes of all logic in Jupyter Notebooks to verify "Manager Logic" before script formalization.

- [ ] **Cloud Provisioning:** Establish an AWS RDS (PostgreSQL) instance and secure the ETL pipeline for automated weekly data migration.

### Phase 2: From Static Fixture Difficulty Rating (FDR) to Dynamic Modifiers

- [x] **Feature Engineering:** Replace standard 1–5 FDR with weighted heuristic score ($0.7 \times \text{Goals Conceded} + 0.3 \times \text{BPS Conceded}$) to capture both "leaky" and "passive" opponent behaviors.
- [x] **Efficiency Normalization:** Implement ICT per 90 metrics to neutralize bias toward high-minute starters and identify high-upside "super-subs."

- [x] **The Interaction Logic:** Calculate the Final Opportunity Score ($Player\_Power \times Opponent\_Multiplier$) to map quality against fixture vulnerability.

- [ ] **Script Refactoring:** Transition "Whiteboard" notebook code into modular, generic .py files for AWS Lambda automation.

### Phase 3: Predictive Modeling (The xP Engine)

- [ ] **Model Development:** Train a Regression model (e.g., Random Forest or XGBoost) to forecast Expected Points ($xP$).

- [ ] **Feature Importance:** Use the logic from Phase 2 as a primary feature to guide the ML model's learning.

- [ ] **Validation:** Back-test the model against previous Gameweeks to measure Mean Absolute Error (MAE).

### Phase 4: Prescriptive Optimization (The Solver)
- [ ] **Lineup Optimization:** Implement a Linear Programming (LP) solver using the PuLP library to maximize $xP$ under budget (£100m) and formation constraints.

- [ ] **Deployment:** Build an interactive Streamlit Dashboard to visualize the "Optimal 11" and allow users to toggle between "Safe" and "Risky" (Differential) strategies.

---

### Future Scope (Version 2.0)

**Position-Specific Weighting:** Customizing player power ratings (e.g., weighting Threat for Forwards vs. Creativity for Midfielders).

**Live xG Integration:** Moving from proxy metrics (Goals/BPS) to real-time xG and xA data as the model matures.

**Transfer Market Simulation:** Adding logic to suggest the most impactful "One Transfer" based on the remaining budget.

---

## Design Philosophy (v2): 

## Why I’m Moving Beyond Standard FDR

When I started this project, I realized that relying on the official FPL Fixture Difficulty Rating (FDR) felt like using a simple tool for a complex task. Here is why I decided to modify my approach:

#### 1. Moving Beyond Categorical "Bluntness"
The standard 1–5 FDR is a categorical metric that doesn't tell the whole story. A "Difficulty 4" against a team with a high defensive line is a completely different challenge than a "Difficulty 4" against a deep-sitting low block. I wanted a model that understands vulnerability, not just "strength."

**Solution:** I am building a system that replaces these integers with Continuous Multipliers ($0.8x$ to $1.5x$) based on rolling defensive performance.

### 2. Normalizing for "True" Impact (ICT per 90)
To avoid the "Total Points Bias", where stable but average players outrank high-ceiling rotation risks I implemented Efficiency Normalization.

**The Logic:** By calculating ICT per 90, the model identifies players who are productive whenever they are on the pitch, regardless of their total minutes.

**The Threshold:** I apply a 270-minute (3 full games) to ensure the efficiency stats are statistically significant before they reach the cloud.

#### 3. The Power of a Hybrid Approach (ML + Rule-Based Logic)
I chose to combine Machine Learning with Rule-Based Logic Math because I believe they solve two different problems:

* **Why I use ML:** I use models like XGBoost to act as my scout. The ML handles the heavy lifting—finding non-linear patterns in thousands of data points (like xG, Threat, and ICT Index) to give me an unbiased prediction of expected points ($xP$).

* **Why I use Rule-Based Logic:** Pure ML can sometimes not adapt to sudden real-world changes. I use my own equations (like my Risky vs. Safe scores) to act as the "Manager." This allows me to inject logic-based risk management and domain expertise into the final decision.


---

## ⚠️ Disclaimer
**Note:** This roadmap is subject to change based on sudden injuries, unexpected VAR decisions, and managers being sacked in the morning. Use these predictions at your own risk; the creator is not responsible for any broken keyboards or triple-captain disasters.