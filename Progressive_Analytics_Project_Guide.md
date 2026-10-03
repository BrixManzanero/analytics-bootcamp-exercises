# Progressive Analytics Project Guide

Analytics Solutions Bootcamp | Learner handout

Build one analytics solution and improve it as you complete the modules. Use the seven INFORMS domains below as the sections of your project report. Update earlier decisions whenever your data, findings, or feedback changes.

**Keep it manageable:** Choose one process, one primary user, and one decision to support. A working pipeline and a useful analysis or dashboard can be your core solution. Add ML or AI when it helps answer your question or when assigned by your instructor.

**Project details:** Project title: [Enter title]   Group: [Enter group]

Members and responsibilities: [Enter names and roles]

## 1 Business Problem Framing

Explain the real situation and the improvement that matters to the people involved.

- What process has a problem or opportunity? Who experiences it, and who can act on your findings?

- What decision should improve? Define your scope, constraints, and a measurable business success criterion.

**Add to your project:** A short problem statement, intended user, scope, and success measure.

**Example:** A small retailer needs to decide which products to reorder. Success could mean reducing stockout days from a measured baseline to an agreed target. Label proposed targets and assumptions clearly.

**Ready to move forward when:** Your group can explain why the project matters without naming a tool.

## 2 Analytics Problem Framing

Translate the business need into questions that data can answer.

- What two or three questions will support the decision? What output will the user need?

- Which measures will you calculate, at what level of detail, and over what period? How will you judge whether the output works?

**Add to your project:** An analytics objective, questions, metric definitions, and acceptance criteria.

**Example:** Which products have the most stockout days per month? Calculate days with zero closing stock per product. Check computed totals against a manually reviewed sample. Business impact is evaluated separately after use.

**Ready to move forward when:** Each question links to the business decision, and your criteria can be tested.

## Prepare the data and choose an approach

## 3 Data

Find suitable data and prepare a repeatable path from source to usable output.

- Where will the data come from? Record the source, access conditions, coverage, and what one row represents.

- Which fields and keys answer your questions? Check missing values, duplicates, invalid values, and join relationships.

- How will you ingest, clean, transform, store, and refresh the data? Keep the raw source and document cleaning decisions.

- Who owns the data and may access it? Identify sensitive fields and use public, permitted, or appropriately de-identified data.

**Add to your project:** A source list, small data dictionary, pipeline diagram, preparation scripts, and a data quality summary.

**Example:** For the retailer, sales records alone cannot confirm stockouts. You also need inventory history. If it is unavailable, narrow the question to sales patterns and document the limitation.

**Ready to move forward when:** The prepared data supports your questions, and another member can rerun the preparation.

**Data dictionary fields:** Field name | Meaning | Data type | Example | Key or validation rule

**Quality evidence:** Record the rule, rows checked, rows failing, action taken, and result after cleaning. Retain or quarantine questionable records when appropriate; do not silently drop them.

## 4 Methodology Selection

Choose the simplest approach that can answer your analytics questions.

- Will summaries, comparisons, statistical analysis, a dashboard, prediction, or another method meet the need?

- What simple baseline or alternative will you compare against? Why does your chosen approach fit the data, time, and skills available?

- What assumptions, limitations, and evaluation measures will you use? Select these before reviewing final results.

**Add to your project:** A short method justification, baseline, tool choices, and evaluation plan.

**Example:** Start with a product-level stockout summary. Consider demand forecasting only if you have enough reliable historical data and a clear planning need.

**Ready to move forward when:** You can explain why the method is appropriate and how you will evaluate it.

For ML, plan separate training and evaluation data; for time-based prediction, preserve time order. For generative AI, prepare representative questions, expected evidence, and checks for unsupported answers.

## Build and put the solution into use

## 5 Analytics and Model Development

Create the solution, check its correctness, and explain what the results mean.

- What patterns or quality issues appear during exploration? Do they change your earlier assumptions?

- Can you build the agreed queries, analysis, dashboard, or model from the prepared data?

- Do calculations and outputs pass your acceptance criteria? Compare with the baseline and investigate errors.

- What findings support a practical action? Explain uncertainty, limitations, and what the data cannot establish.

**Add to your project:** Working code or queries, the analysis or dashboard, validation evidence, and a short findings summary.

**Ready to move forward when:** Outputs are reproducible, key calculations have been checked, and recommendations follow from the evidence.

**Write each finding as:** Observation → Supporting measure or chart → Suggested action → Limitation

**If you use ML or AI:** Evaluate on data or questions not used to tune the solution. Prevent data leakage, review representative errors, and report weaknesses. Check AI claims against their source; keep human review for consequential outputs.

## 6 Deployment

Make the solution usable by its intended audience.

- How will someone access and use it? Show the steps from data input or refresh to the final output and decision.

- Can another member follow your setup and usage instructions? Check dependencies, permissions, filters, and expected outputs.

- How will you handle a failed refresh or incorrect result? Describe a fallback and how to return to the last working version.

- Who has tried it, what feedback did they give, and what did you improve?

**Add to your project:** A usable prototype, a README or user guide, and a brief user test record.

**Ready to move forward when:** Another person can run or use the prototype and complete the intended task.

**For the bootcamp:** A local demonstration or controlled prototype is sufficient for this guide. Explain what additional work would be needed for operational use. If a classmate acts as the user, label the feedback as a simulated user test.

**User test record:** Task attempted | Expected result | Actual result | Feedback | Change made

## Maintain and complete the project

## 7 Analytics Solution Lifecycle Management

Plan how the solution will stay useful and reliable after the demonstration.

- Who will maintain it, how often should it refresh, and which quality or performance checks will run?

- What failure threshold triggers action? Who investigates, and when should the solution be revised or retired?

- How will you check whether the intended business benefit actually occurs?

**Add to your project:** A short maintenance plan covering owner, schedule, check, trigger, and response.

**Ready to move forward when:** Someone is responsible for each check, with a clear action if it fails.

**Example:** Data lead checks each refresh for duplicate product-date records. If any are found, pause publication, investigate, and keep the last validated output. The business user reviews stockout trends monthly; benefits remain unverified until observed in use.

## Suggested progress by module

Keep one report and one shared project folder. The domains overlap; governance and evaluation begin early and are strengthened as you learn.

| Modules | Build or update | Main domains |
| --- | --- | --- |
| 1 and 2 | Problem, questions, sources, initial data review | 1, 2, 3 |
| 3 | Repeatable ingestion, transformations, data model | 3, 4, 5 |
| 4 | EDA, defined metrics, dashboard, initial findings | 4, 5 |
| 5 | Evaluate results and a suitable ML extension, if relevant | 4, 5 |
| 6 | Ownership, glossary, quality rules, access, lineage | 3, 6, 7 |
| 7 | Assess or test an AI extension; complete demo and handover | 4, 5, 6, 7 |

## Final handover checklist

Include your seven-section report; source links and data dictionary; runnable code and setup instructions; working output; validation results and limitations; and user and maintenance guides. Note each member’s contribution and identify unfinished work.

At each checkpoint, record: completed work, evidence link, feedback received, next action, and owner. Confirm submission dates and any additional requirements with your instructor.

Framework reference: [INFORMS Analytics Framework](https://www.informs.org/Professional-Development/INFORMS-Analytics-Framework). This learner guide adapts the seven domains to the bootcamp’s Modules 1–7; the activities and checkpoint mapping are suggested teaching guidance.
