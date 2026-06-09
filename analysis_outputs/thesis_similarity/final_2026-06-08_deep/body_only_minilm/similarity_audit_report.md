# Thesis Similarity And Citation Audit

## Scope

- Thesis chunks checked: 260
- Source/internal chunks scanned: 7208
- External similarity candidates: 79
- High-priority external candidates: 0
- Internal self-similarity candidates: 0
- Citation issues: 60
- Semantic layer: semantic model: sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2

This local audit is not a formal university plagiarism certificate. It cannot compare against closed student-paper databases.

## High-Priority External Candidates

No high-priority external-source candidates crossed the configured thresholds.

## Top External Similarity Candidates

- `analysis_outputs\thesis_similarity\final_2026-06-08_deep\inputs\google_doc_body_chapters_1_5.txt:124` vs `docs\thesis\sources\Analyzing_Uncertainty_Quantification_in_Statistica.pdf:1` method=semantic_embedding score=0.8088 exact_shared=0
  - Thesis: Класичні роботи з electricity price forecasting часто оцінюють моделі через MAE, RMSE, MAPE або quantile losses. Такі метрики корисні для порівняння прогнозів, але вони не відповідають безпосередньо на питання, чи кращим буде battery schedule. Lago et al. (2021) показують важливі
  - Source: choice data or parameter calibration in the electricity price forecasting literature several authors have focused on developing probabilistic forecasting mod- els for power prices see e g 1 2 and 3 for overviews for general overviews of probabilistic interval forecasting see 4 an
- `docs\thesis\chapters\02-literature-review.md:21` vs `docs\thesis\sources\Analyzing_Uncertainty_Quantification_in_Statistica.pdf:1` method=semantic_embedding score=0.8013 exact_shared=0
  - Thesis: Класичні роботи з electricity price forecasting часто оцінюють моделі через MAE, RMSE, MAPE або quantile losses. Такі метрики корисні для порівняння прогнозів, але вони не відповідають безпосередньо на питання, чи кращим буде battery schedule. Lago et al. (2021) показують важливі
  - Source: choice data or parameter calibration in the electricity price forecasting literature several authors have focused on developing probabilistic forecasting mod- els for power prices see e g 1 2 and 3 for overviews for general overviews of probabilistic interval forecasting see 4 an
- `analysis_outputs\thesis_similarity\final_2026-06-08_deep\inputs\google_doc_body_chapters_1_5.txt:107` vs `docs\thesis\sources\conversation-source-capture-2026-05-08.md:1` method=semantic_embedding score=0.795 exact_shared=0
  - Thesis: Ринок на добу наперед задає погодинні price signals, які можуть бути використані для планування заряду й розряду BESS. Для України така задача має практичну вагу через потребу в гнучкості, інтеграції відновлюваної генерації, локальному балансуванні та зменшенні вартості пікових г
  - Source: forecasts by realized dispatch revenue against perfect-foresight schedules and considers ageing costs local note dfki-2025-bess-dispatch-forecast-impact md' newly captured policy and market-coupling sources status source why it matters current use include energy community ukraine
- `docs\thesis\chapters\02-literature-review.md:5` vs `docs\thesis\sources\conversation-source-capture-2026-05-08.md:1` method=semantic_embedding score=0.795 exact_shared=0
  - Thesis: Ринок на добу наперед задає погодинні price signals, які можуть бути використані для планування заряду й розряду BESS. Для України така задача має практичну вагу через потребу в гнучкості, інтеграції відновлюваної генерації, локальному балансуванні та зменшенні вартості пікових г
  - Source: forecasts by realized dispatch revenue against perfect-foresight schedules and considers ageing costs local note dfki-2025-bess-dispatch-forecast-impact md' newly captured policy and market-coupling sources status source why it matters current use include energy community ukraine
- `analysis_outputs\thesis_similarity\final_2026-06-08_deep\inputs\google_doc_body_chapters_1_5.txt:197` vs `docs\thesis\sources\2406.17085v2-perturbed-decision-focused-learning-energy-storage.pdf:1` method=semantic_embedding score=0.7914 exact_shared=0
  - Thesis: Decision-Focused Learning у повному сенсі може означати навчання моделі через differentiable optimization layer або task-aware surrogate loss. Smart Predict-then-Optimize у Elmachtoub and Grigas (2022) формалізує проблему розриву між prediction loss і downstream objective [12]. M
  - Source: however such approaches fail to account for the optimization structure of energy storage leading to degraded prediction performance c decision-focused learning decision-focused learning has gained increasing interest in overcoming the limitations of the mpc-type methods in which 
- `docs\thesis\chapters\02-literature-review.md:72` vs `docs\thesis\sources\2307.13565v4-decision-focused-learning-survey.pdf:1` method=semantic_embedding score=0.7868 exact_shared=0
  - Thesis: Decision-Focused Learning у повному сенсі може означати навчання моделі через differentiable optimization layer або task-aware surrogate loss. Smart Predict-then-Optimize у Elmachtoub and Grigas (2022) формалізує проблему розриву між prediction loss і downstream objective [[12]](
  - Source: prescriptive modelings are not isolated but rather deeply interconnected and hence should ideally be modeled jointly this is the goal of the decision-focused learning dfl paradigm which directly trains the ml model to make predictions that lead to good decisions in other words df
- `analysis_outputs\thesis_similarity\final_2026-06-08_deep\inputs\google_doc_body_chapters_1_5.txt:107` vs `docs\thesis\sources\iea-2024-ukraines-energy-security-and-coming-winter.pdf:1` method=semantic_embedding score=0.7827 exact_shared=0
  - Thesis: Ринок на добу наперед задає погодинні price signals, які можуть бути використані для планування заряду й розряду BESS. Для України така задача має практичну вагу через потребу в гнучкості, інтеграції відновлюваної генерації, локальному балансуванні та зменшенні вартості пікових г
  - Source: of their power consumption ukraine's energy security and the coming winter an energy action plan page 30 i ea cc by 4 0 through reform of the price cap in the ukrainian electricity market introducing instead a system whereby a social consumption volume is charged at a subsidized 
- `analysis_outputs\thesis_similarity\final_2026-06-08_deep\inputs\google_doc_body_chapters_1_5.txt:705` vs `docs\thesis\sources\deep-similarity-and-citation-audit-2026-05-20.md:1` method=semantic_embedding score=0.7823 exact_shared=0
  - Thesis: Рисунок 4.1 робить headline result зрозумілим без довгого переліку проміжних експериментів. Lower regret означає меншу втрату economic value відносно oracle. V2+ знижує mean regret на 43.73% проти strict baseline і на 15.31% проти frozen V2. Ключову result card наведено на рисунк
  - Source: перевіряються окремо як обмежувальні метрики тоді regret стратегії docs thesis chapters 03-methodology md 342' heading 3 6 метрики оцінювання і роль regret' text нульовий regret означає що стратегія досягла oracle-equivalent value під тими самими constraints чим більший regret ти
- `analysis_outputs\thesis_similarity\final_2026-06-08_deep\inputs\google_doc_body_chapters_1_5.txt:197` vs `docs\thesis\sources\2307.13565v4-decision-focused-learning-survey.pdf:1` method=semantic_embedding score=0.7812 exact_shared=0
  - Thesis: Decision-Focused Learning у повному сенсі може означати навчання моделі через differentiable optimization layer або task-aware surrogate loss. Smart Predict-then-Optimize у Elmachtoub and Grigas (2022) формалізує проблему розриву між prediction loss і downstream objective [12]. M
  - Source: prescriptive modelings are not isolated but rather deeply interconnected and hence should ideally be modeled jointly this is the goal of the decision-focused learning dfl paradigm which directly trains the ml model to make predictions that lead to good decisions in other words df
- `docs\thesis\chapters\02-literature-review.md:5` vs `docs\thesis\sources\iea-2024-ukraines-energy-security-and-coming-winter.pdf:1` method=semantic_embedding score=0.7786 exact_shared=0
  - Thesis: Ринок на добу наперед задає погодинні price signals, які можуть бути використані для планування заряду й розряду BESS. Для України така задача має практичну вагу через потребу в гнучкості, інтеграції відновлюваної генерації, локальному балансуванні та зменшенні вартості пікових г
  - Source: of their power consumption ukraine's energy security and the coming winter an energy action plan page 30 i ea cc by 4 0 through reform of the price cap in the ukrainian electricity market introducing instead a system whereby a social consumption volume is charged at a subsidized 
- `docs\thesis\chapters\04-results-and-discussion.md:40` vs `docs\thesis\sources\deep-similarity-and-citation-audit-2026-05-20.md:1` method=semantic_embedding score=0.7782 exact_shared=0
  - Thesis: Рисунок 4.1 робить headline result зрозумілим без довгого переліку проміжних експериментів. Lower regret означає меншу втрату economic value відносно oracle. V2+ знижує mean regret на 43.73% проти strict baseline і на 15.31% проти frozen V2.
  - Source: перевіряються окремо як обмежувальні метрики тоді regret стратегії docs thesis chapters 03-methodology md 342' heading 3 6 метрики оцінювання і роль regret' text нульовий regret означає що стратегія досягла oracle-equivalent value під тими самими constraints чим більший regret ти
- `docs\thesis\chapters\01-project-overview.md:5` vs `docs\thesis\sources\conversation-source-capture-2026-05-08.md:1` method=semantic_embedding score=0.7732 exact_shared=0
  - Thesis: Українська енергосистема у 2026 році потребує інструментів, які допомагають працювати з волатильністю, дефіцитом маневровості та зростанням ролі систем накопичення енергії. BESS може переносити енергію між годинами з різною ціною, але практична цінність такого переносу виникає ли
  - Source: forecasts by realized dispatch revenue against perfect-foresight schedules and considers ageing costs local note dfki-2025-bess-dispatch-forecast-impact md' newly captured policy and market-coupling sources status source why it matters current use include energy community ukraine
- `docs\thesis\chapters\02-literature-review.md:21` vs `docs\thesis\sources\2008.08004v2-electricity-price-forecasting-review-benchmark.pdf:1` method=semantic_embedding score=0.7728 exact_shared=0
  - Thesis: Класичні роботи з electricity price forecasting часто оцінюють моделі через MAE, RMSE, MAPE або quantile losses. Такі метрики корисні для порівняння прогнозів, але вони не відповідають безпосередньо на питання, чи кращим буде battery schedule. Lago et al. (2021) показують важливі
  - Source: in future studies keywords electricity price forecasting deep learning open-access benchmark forecast evaluation best practices for price forecasting 1 introduction the increasing penetration of renewable energy sources res in today's power systems makes electricity gen- eration 
- `analysis_outputs\thesis_similarity\final_2026-06-08_deep\inputs\google_doc_body_chapters_1_5.txt:5` vs `docs\thesis\sources\iea-2024-ukraines-energy-security-and-coming-winter.pdf:1` method=semantic_embedding score=0.7718 exact_shared=0
  - Thesis: Українська енергосистема у 2026/27 році і надалі потребує інструментів, які допомагають працювати з стресом, аварійністю, волатильністю, дефіцитом маневреності та зростанням ролі систем накопичення енергії. BESS може транспортувати енергію між годинами з різною ціною, але практич
  - Source: writing ukraine's energy security and the coming winter ukraine's energy system under attack page 13 i ea cc by 4 0 estimated electricity generation capacity available to ukraine at selected times since 2022 sources undp 2023 update on the energy damage assessment june 2023 towar
- `analysis_outputs\thesis_similarity\final_2026-06-08_deep\inputs\google_doc_body_chapters_1_5.txt:5` vs `docs\thesis\sources\iea-2024-ukraines-energy-security-and-coming-winter.pdf:1` method=semantic_embedding score=0.7708 exact_shared=0
  - Thesis: Українська енергосистема у 2026/27 році і надалі потребує інструментів, які допомагають працювати з стресом, аварійністю, волатильністю, дефіцитом маневреності та зростанням ролі систем накопичення енергії. BESS може транспортувати енергію між годинами з різною ціною, але практич
  - Source: as the transfer of decommissioned equipment from ukraine's european neighbours however investment in new capacity and the restoration of either damaged or destroyed ukraine's energy security and the coming winter an energy action plan page 34 i ea cc by 4 0 assets may impose unwa
- `analysis_outputs\thesis_similarity\final_2026-06-08_deep\inputs\google_doc_body_chapters_1_5.txt:1048` vs `docs\thesis\sources\deep-similarity-and-citation-audit-2026-05-20.md:1` method=semantic_embedding score=0.7705 exact_shared=0
  - Thesis: Перший висновок: для BESS arbitrage forecast-only метрики недостатні. Практична якість визначається не тим, наскільки красиво модель передбачила ціну, а тим, скільки economic value schedule втрачає відносно oracle LP після SOC, power, efficiency і timing constraints. Тому regret/
  - Source: і decision layer його задача полягає не в тому щоб довести перевагу моделі за mae rmse а в тому щоб виправити систематичні forecast-зсуви які найбільше шкодять downstream arbitrage decision для bess-арбітражу однакова forecast помилка в різні години не має однакової ціни помилка 
- `analysis_outputs\thesis_similarity\final_2026-06-08_deep\inputs\google_doc_body_chapters_1_5.txt:124` vs `docs\thesis\sources\2008.08004v2-electricity-price-forecasting-review-benchmark.pdf:1` method=semantic_embedding score=0.77 exact_shared=0
  - Thesis: Класичні роботи з electricity price forecasting часто оцінюють моделі через MAE, RMSE, MAPE або quantile losses. Такі метрики корисні для порівняння прогнозів, але вони не відповідають безпосередньо на питання, чи кращим буде battery schedule. Lago et al. (2021) показують важливі
  - Source: in future studies keywords electricity price forecasting deep learning open-access benchmark forecast evaluation best practices for price forecasting 1 introduction the increasing penetration of renewable energy sources res in today's power systems makes electricity gen- eration 
- `analysis_outputs\thesis_similarity\final_2026-06-08_deep\inputs\google_doc_body_chapters_1_5.txt:124` vs `docs\thesis\sources\2508.04875-pricefm-electricity-price-forecasting.pdf:1` method=semantic_embedding score=0.7689 exact_shared=0
  - Thesis: Класичні роботи з electricity price forecasting часто оцінюють моделі через MAE, RMSE, MAPE або quantile losses. Такі метрики корисні для порівняння прогнозів, але вони не відповідають безпосередньо на питання, чи кращим буде battery schedule. Lago et al. (2021) показують важливі
  - Source: pricefm foundation model for probabilistic electricity price forecasting runyao yu1 2 3 chenhui gu 1 jochen stiasny 1 qingsong wen 4 wasim sarwar dilov 3 lianlian qi 5 jochen l cremer 1 2 abstract electricity price forecasting in europe presents unique challenges due to the conti
- `docs\thesis\chapters\02-literature-review.md:21` vs `docs\thesis\sources\2008.08004v2-electricity-price-forecasting-review-benchmark.pdf:1` method=semantic_embedding score=0.7674 exact_shared=0
  - Thesis: Класичні роботи з electricity price forecasting часто оцінюють моделі через MAE, RMSE, MAPE або quantile losses. Такі метрики корисні для порівняння прогнозів, але вони не відповідають безпосередньо на питання, чи кращим буде battery schedule. Lago et al. (2021) показують важливі
  - Source: nascimento t pinto z vale day-ahead electricity market price forecasting using arti cial neural network with spearman data correlation in proceedings of the 2019 ieee powertech conference 2019 pp 1 6 doi 10 1109 ptc 2019 8810618 131 d kotur m zarkovic neural network models for el
- `docs\thesis\chapters\02-literature-review.md:21` vs `docs\thesis\sources\Analyzing_Uncertainty_Quantification_in_Statistica.pdf:1` method=semantic_embedding score=0.7659 exact_shared=0
  - Thesis: Класичні роботи з electricity price forecasting часто оцінюють моделі через MAE, RMSE, MAPE або quantile losses. Такі метрики корисні для порівняння прогнозів, але вони не відповідають безпосередньо на питання, чи кращим буде battery schedule. Lago et al. (2021) показують важливі
  - Source: x chen z y dong k meng y xu k p wong and h w ngan electricity price forecasting with extreme learning machine and boot- strapping ieee transactions on power systems vol 27 no 4 pp 2055 2062 2012 9 a lipiecki b uniejewski and r weron postprocessing of point predictions for probabi
- `analysis_outputs\thesis_similarity\final_2026-06-08_deep\inputs\google_doc_body_chapters_1_5.txt:197` vs `docs\thesis\sources\2305.00362v1-electricity-price-prediction-ess-arbitrage-dfl.pdf:1` method=semantic_embedding score=0.7651 exact_shared=0
  - Thesis: Decision-Focused Learning у повному сенсі може означати навчання моделі через differentiable optimization layer або task-aware surrogate loss. Smart Predict-then-Optimize у Elmachtoub and Grigas (2022) формалізує проблему розриву між prediction loss і downstream objective [12]. M
  - Source: of prediction accuracy and the embedding framework is further extended to the general economic dispatch in 6 7 in 4 the smart predict then optimize spo loss is proposed to learn linear predictor parameters by linear programming to solve the shortest path and portfolio optimizatio
- `docs\thesis\chapters\02-literature-review.md:21` vs `docs\thesis\sources\2508.04875-pricefm-electricity-price-forecasting.pdf:1` method=semantic_embedding score=0.7647 exact_shared=0
  - Thesis: Класичні роботи з electricity price forecasting часто оцінюють моделі через MAE, RMSE, MAPE або quantile losses. Такі метрики корисні для порівняння прогнозів, але вони не відповідають безпосередньо на питання, чи кращим буде battery schedule. Lago et al. (2021) показують важливі
  - Source: pricefm foundation model for probabilistic electricity price forecasting runyao yu1 2 3 chenhui gu 1 jochen stiasny 1 qingsong wen 4 wasim sarwar dilov 3 lianlian qi 5 jochen l cremer 1 2 abstract electricity price forecasting in europe presents unique challenges due to the conti
- `analysis_outputs\thesis_similarity\final_2026-06-08_deep\inputs\google_doc_body_chapters_1_5.txt:124` vs `docs\thesis\sources\2305.00362v1-electricity-price-prediction-ess-arbitrage-dfl.pdf:1` method=semantic_embedding score=0.7646 exact_shared=0
  - Thesis: Класичні роботи з electricity price forecasting часто оцінюють моделі через MAE, RMSE, MAPE або quantile losses. Такі метрики корисні для порівняння прогнозів, але вони не відповідають безпосередньо на питання, чи кращим буде battery schedule. Lago et al. (2021) показують важливі
  - Source: 1 electricity price prediction for energy storage system arbitrage a decision-focused approach linwei sang yinliang xu senior member ieee huan long member ieee qinran hu senior member ieee hongbin sun fellow ieee abstract electricity price prediction plays a vital role in energy 
- `analysis_outputs\thesis_similarity\final_2026-06-08_deep\inputs\google_doc_body_chapters_1_5.txt:434` vs `docs\thesis\sources\deep-similarity-and-citation-audit-2026-05-20.md:1` method=semantic_embedding score=0.764 exact_shared=0
  - Thesis: Таблиця 3.2. Метрики та нумеровані вирази методології Метрика Вираз Роль у висновку Schedule value 3.1 Оцінює економічну цінність hourly schedule SOC dynamics у LP 3.1.1 Штраф, degradation proxy для schedule value Regret 3.2 Порівнює schedule з oracle LP Mean improvement 3.3 Норм
  - Source: що й baseline прогнозна траєкторія перетворюється на schedule через lp dispatch optimizer після чого schedule оцінюється на realized prices це дозволяє порівнювати моделі за тим що має економічний сенс для bess docs thesis chapters 03-methodology md 126' heading 3 4 forecast-to-s
- `analysis_outputs\thesis_similarity\final_2026-06-08_deep\inputs\google_doc_body_chapters_1_5.txt:124` vs `docs\thesis\sources\2008.08004v2-electricity-price-forecasting-review-benchmark.pdf:1` method=semantic_embedding score=0.7638 exact_shared=0
  - Thesis: Класичні роботи з electricity price forecasting часто оцінюють моделі через MAE, RMSE, MAPE або quantile losses. Такі метрики корисні для порівняння прогнозів, але вони не відповідають безпосередньо на питання, чи кращим буде battery schedule. Lago et al. (2021) показують важливі
  - Source: nascimento t pinto z vale day-ahead electricity market price forecasting using arti cial neural network with spearman data correlation in proceedings of the 2019 ieee powertech conference 2019 pp 1 6 doi 10 1109 ptc 2019 8810618 131 d kotur m zarkovic neural network models for el
- `docs\thesis\chapters\02-literature-review.md:72` vs `docs\thesis\sources\2406.17085v2-perturbed-decision-focused-learning-energy-storage.pdf:1` method=semantic_embedding score=0.7632 exact_shared=0
  - Thesis: Decision-Focused Learning у повному сенсі може означати навчання моделі через differentiable optimization layer або task-aware surrogate loss. Smart Predict-then-Optimize у Elmachtoub and Grigas (2022) формалізує проблему розриву між prediction loss і downstream objective [[12]](
  - Source: however such approaches fail to account for the optimization structure of energy storage leading to degraded prediction performance c decision-focused learning decision-focused learning has gained increasing interest in overcoming the limitations of the mpc-type methods in which 
- `analysis_outputs\thesis_similarity\final_2026-06-08_deep\inputs\google_doc_body_chapters_1_5.txt:197` vs `docs\thesis\sources\2307.13565v4-decision-focused-learning-survey.pdf:1` method=semantic_embedding score=0.7614 exact_shared=0
  - Thesis: Decision-Focused Learning у повному сенсі може означати навчання моделі через differentiable optimization layer або task-aware surrogate loss. Smart Predict-then-Optimize у Elmachtoub and Grigas (2022) формалізує проблему розриву між prediction loss і downstream objective [12]. M
  - Source: component of the ml model in this integration of prediction and optimization a key challenge is differentiating through the optimization problem an additional challenge arises from decision models operating on discrete variables which produce discontinuous mappings and hinder gra
- `analysis_outputs\thesis_similarity\final_2026-06-08_deep\inputs\google_doc_body_chapters_1_5.txt:5` vs `docs\thesis\sources\iea-2024-ukraines-energy-security-and-coming-winter.pdf:1` method=semantic_embedding score=0.7602 exact_shared=0
  - Thesis: Українська енергосистема у 2026/27 році і надалі потребує інструментів, які допомагають працювати з стресом, аварійністю, волатильністю, дефіцитом маневреності та зростанням ролі систем накопичення енергії. BESS може транспортувати енергію між годинами з різною ціною, але практич
  - Source: since the spring of 2024 the targeting of energy infrastructure has had wide-ranging consequences for the provision of energy to ukrainian households and other consumers over the course of 2022-23 about half of ukraine's power generation capacity was either occupied by russian fo
- `analysis_outputs\thesis_similarity\final_2026-06-08_deep\inputs\google_doc_body_chapters_1_5.txt:181` vs `docs\thesis\sources\2106.08702v1.pdf:1` method=semantic_embedding score=0.7597 exact_shared=0
  - Thesis: бути несправедливим. Vykhodtsev et al. (2022) додатково пояснюють, що спрощене power-energy представлення батареї є придатним для techno-economic studies, але не замінює повний electrochemical digital twin [25]. Strict LP/oracle evaluator виконує дві ролі. По-перше, він оцінює va
  - Source: capital cost of the battery he et al 44 obtained a more accurate estimate of the 24 energy arbitrage business case for the california day-ahead electricity market overall the energy arbitrage operation considering ageing of the battery gives a better estimate in the cost bene t a
- `docs\thesis\chapters\05-conclusions-and-recommendations.md:22` vs `docs\thesis\sources\deep-similarity-and-citation-audit-2026-05-20.md:1` method=semantic_embedding score=0.7579 exact_shared=0
  - Thesis: Перший висновок: у задачі BESS arbitrage головним є не прогноз як такий, а якість рішення після перетворення price signal у feasible schedule. Forecast-only метрики залишаються корисними diagnostics, але вони не відповідають на питання, скільки economic value втрачено відносно or
  - Source: і decision layer його задача полягає не в тому щоб довести перевагу моделі за mae rmse а в тому щоб виправити систематичні forecast-зсуви які найбільше шкодять downstream arbitrage decision для bess-арбітражу однакова forecast помилка в різні години не має однакової ціни помилка 

## Top Internal Self-Similarity Candidates

No internal self-similarity candidates crossed the configured thresholds.

## Citation Consistency Issues

- `docs\thesis\chapters\02-literature-review.md:1` missing-source-anchor: source-1 is cited but absent
- `docs\thesis\chapters\02-literature-review.md:1` missing-source-anchor: source-2 is cited but absent
- `docs\thesis\chapters\02-literature-review.md:1` missing-source-anchor: source-3 is cited but absent
- `docs\thesis\chapters\02-literature-review.md:1` missing-source-anchor: source-4 is cited but absent
- `docs\thesis\chapters\02-literature-review.md:1` missing-source-anchor: source-5 is cited but absent
- `docs\thesis\chapters\02-literature-review.md:1` missing-source-anchor: source-6 is cited but absent
- `docs\thesis\chapters\02-literature-review.md:1` missing-source-anchor: source-7 is cited but absent
- `docs\thesis\chapters\02-literature-review.md:1` missing-source-anchor: source-8 is cited but absent
- `docs\thesis\chapters\02-literature-review.md:1` missing-source-anchor: source-9 is cited but absent
- `docs\thesis\chapters\02-literature-review.md:1` missing-source-anchor: source-10 is cited but absent
- `docs\thesis\chapters\02-literature-review.md:1` missing-source-anchor: source-11 is cited but absent
- `docs\thesis\chapters\02-literature-review.md:1` missing-source-anchor: source-12 is cited but absent
- `docs\thesis\chapters\02-literature-review.md:1` missing-source-anchor: source-13 is cited but absent
- `docs\thesis\chapters\02-literature-review.md:1` missing-source-anchor: source-14 is cited but absent
- `docs\thesis\chapters\02-literature-review.md:1` missing-source-anchor: source-15 is cited but absent
- `docs\thesis\chapters\02-literature-review.md:1` missing-source-anchor: source-16 is cited but absent
- `docs\thesis\chapters\02-literature-review.md:1` missing-source-anchor: source-17 is cited but absent
- `docs\thesis\chapters\02-literature-review.md:1` missing-source-anchor: source-18 is cited but absent
- `docs\thesis\chapters\02-literature-review.md:1` missing-source-anchor: source-19 is cited but absent
- `docs\thesis\chapters\02-literature-review.md:1` missing-source-anchor: source-20 is cited but absent
- `docs\thesis\chapters\02-literature-review.md:1` missing-source-anchor: source-21 is cited but absent
- `docs\thesis\chapters\02-literature-review.md:1` missing-source-anchor: source-22 is cited but absent
- `docs\thesis\chapters\02-literature-review.md:1` missing-source-anchor: source-23 is cited but absent
- `docs\thesis\chapters\02-literature-review.md:1` missing-source-anchor: source-24 is cited but absent
- `docs\thesis\chapters\02-literature-review.md:1` missing-source-anchor: source-25 is cited but absent
- `docs\thesis\chapters\02-literature-review.md:1` missing-source-anchor: source-26 is cited but absent
- `docs\thesis\chapters\02-literature-review.md:1` missing-source-anchor: source-27 is cited but absent
- `docs\thesis\chapters\02-literature-review.md:1` missing-source-anchor: source-28 is cited but absent
- `docs\thesis\chapters\02-literature-review.md:1` missing-source-anchor: source-29 is cited but absent
- `docs\thesis\chapters\02-literature-review.md:1` missing-source-anchor: source-30 is cited but absent
- `docs\thesis\chapters\02-literature-review.md:1` missing-source-anchor: source-31 is cited but absent
- `docs\thesis\chapters\02-literature-review.md:1` missing-source-anchor: source-32 is cited but absent
- `docs\thesis\chapters\02-literature-review.md:1` missing-source-anchor: source-33 is cited but absent
- `docs\thesis\chapters\02-literature-review.md:1` missing-source-anchor: source-34 is cited but absent
- `docs\thesis\chapters\02-literature-review.md:1` missing-source-anchor: source-35 is cited but absent
- `docs\thesis\chapters\02-literature-review.md:1` missing-source-anchor: source-36 is cited but absent
- `docs\thesis\chapters\02-literature-review.md:1` missing-source-anchor: source-37 is cited but absent
- `docs\thesis\chapters\02-literature-review.md:1` missing-source-anchor: source-38 is cited but absent
- `docs\thesis\chapters\02-literature-review.md:1` missing-source-anchor: source-39 is cited but absent
- `docs\thesis\chapters\02-literature-review.md:1` missing-source-anchor: source-40 is cited but absent
- `docs\thesis\chapters\02-literature-review.md:1` missing-source-anchor: source-41 is cited but absent
- `docs\thesis\chapters\02-literature-review.md:1` missing-source-anchor: source-42 is cited but absent
- `docs\thesis\chapters\02-literature-review.md:1` missing-source-anchor: source-43 is cited but absent
- `docs\thesis\chapters\02-literature-review.md:1` missing-source-anchor: source-44 is cited but absent
- `docs\thesis\chapters\02-literature-review.md:1` missing-source-anchor: source-45 is cited but absent
- `docs\thesis\chapters\02-literature-review.md:1` missing-source-anchor: source-46 is cited but absent
- `docs\thesis\chapters\02-literature-review.md:1` missing-source-anchor: source-47 is cited but absent
- `docs\thesis\chapters\02-literature-review.md:1` missing-source-anchor: source-48 is cited but absent
- `docs\thesis\chapters\02-literature-review.md:1` missing-source-anchor: source-49 is cited but absent
- `docs\thesis\chapters\02-literature-review.md:1` missing-source-anchor: source-50 is cited but absent
- `docs\thesis\chapters\02-literature-review.md:1` missing-source-anchor: source-51 is cited but absent
- `docs\thesis\chapters\02-literature-review.md:1` missing-source-anchor: source-52 is cited but absent
- `docs\thesis\chapters\02-literature-review.md:1` missing-source-anchor: source-53 is cited but absent
- `docs\thesis\chapters\02-literature-review.md:1` missing-source-anchor: source-54 is cited but absent
- `docs\thesis\chapters\02-literature-review.md:1` missing-source-anchor: source-55 is cited but absent
- `docs\thesis\chapters\02-literature-review.md:1` missing-source-anchor: source-56 is cited but absent
- `docs\thesis\chapters\02-literature-review.md:1` missing-source-anchor: source-57 is cited but absent
- `docs\thesis\chapters\02-literature-review.md:1` missing-source-anchor: source-58 is cited but absent
- `docs\thesis\chapters\02-literature-review.md:1` missing-source-anchor: source-59 is cited but absent
- `docs\thesis\chapters\02-literature-review.md:1` missing-source-anchor: source-60 is cited but absent
