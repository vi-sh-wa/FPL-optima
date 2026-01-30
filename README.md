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
- [x] Initial integration with FPL bootstrap-static API.
- [x] Design and implement Team Mapping and data cleaning scripts.
- [ ] Provision AWS RDS (PostgreSQL) instance for cloud data storage.
- [ ] Establish a secure ETL pipeline to move local Python data to the AWS cloud.

### Phase 2: From Static Fixture Difficulty Rating (FDR) to Dynamic Modifiers
- [ ] **Feature Engineering:** Replace standard 1–5 FDR with Split-Difficulty Modifiers (Attack vs. Defense).
- [ ] **Underlying Stats Integration:** Extract "ICT Index" (Influence, Creativity, Threat) to establish player baseline performance.
- [ ] **Historical Scraper:** Build a script to pull Gameweek-by-Gameweek history to calculate Rolling xG (Expected Goals).

### Phase 3: Predictive Modeling (The xP Engine)
- [ ] **Model Development:** Train a Regression model (e.g., Random Forest or XGBoost) to forecast Expected Points ($xP$).
- [ ] **Difficulty Adjustment:** Implement the "Multiplier Effect"—adjusting a player's baseline $xG$ based on the opponent's conceded $xG$ volatility.
- [ ] **Validation:** Back-test the model against previous Gameweeks to measure Mean Absolute Error (MAE).

### Phase 4: Prescriptive Optimization (The Solver)
- [ ] **Lineup Optimization:** Implement a Linear Programming (LP) solver using the PuLP library to maximize $xP$ under budget (£100m) and formation constraints.
- [ ] **Alternatives Engine:** Develop logic to suggest "Next Best" alternatives to allow user-driven customization.
- [ ] **UI/UX Deployment:** Build an interactive Streamlit Dashboard to visualize the "Optimal 11" and allow users to toggle between "Safe" and "Risky" (Differential) strategies.

---

## Design Philosophy: Why I’m Moving Beyond Standard FDR

When I started this project, I realized that relying on the official FPL Fixture Difficulty Rating (FDR) felt like using a simple tool for a complex task. Here is why I decided to modify my approach:

#### 1. Moving Beyond Categorical "Bluntness"
The standard 1–5 FDR is a categorical metric that doesn't tell the whole story. A "Difficulty 4" against a team with a high defensive line is a completely different challenge than a "Difficulty 4" against a deep-sitting low block. I wanted a model that understands vulnerability, not just "strength."

**Solution:** I am building a system that splits difficulty into Attacking and Defensive Modifiers, allowing me to see if a fixture is "Easy" specifically for a Defender or an Attacker.

#### 2. The Power of a Hybrid Approach (ML + Rule-Based Logic)
I chose to combine Machine Learning with Rule-Based Logic Math because I believe they solve two different problems:

* **Why I use ML:** I use models like XGBoost to act as my "Data Scout." The ML handles the heavy lifting—finding non-linear patterns in thousands of data points (like xG, Threat, and ICT Index) to give me an unbiased prediction of expected points ($xP$).
* **Why I use Rule-Based Logic:** Pure ML can sometimes be "brittle" to sudden real-world changes. I use my own equations (like my Risky vs. Safe scores) to act as the "Manager." This allows me to inject logic-based risk management and domain expertise into the final decision.

#### 3. The "Two-Lens" Strategy
By combining these two, I’m not just predicting who might score; I’m optimizing for reliability.
* **ML** gives me the Probability (The "Engine").
* **Rule-Based Logic** gives me the Stability (The "Steering Wheel").

---

## ⚠️ Disclaimer
**Note:** This roadmap is subject to change based on sudden injuries, unexpected VAR decisions, and the general "Bald Fraudulence" of Premier League managers. Use these predictions at your own risk; the creator is not responsible for any broken keyboards or triple-captain disasters.