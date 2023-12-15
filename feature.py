import pandas as pd
import datetime as dt

# df = pd.read_csv('clvall.csv')
# df['date'] = pd.to_datetime(df['creationdate'])
# df['day'] = (df['date'] - dt.datetime.now()).dt.days
# X = df.drop("creationdate", axis='columns').drop("date", axis='columns').drop("reputation", axis='columns').drop("answer_owner_id", axis='columns').drop("sumview", axis='columns')
# y = df[['reputation']]

df = pd.read_csv('reg2.csv')
X = df.drop("count accepted", axis='columns').drop("warm_high", axis='columns').drop("Inactive", axis='columns')
y = df[['count accepted']]
# X = newdf[:, 1:]  # select columns 1 through end
# y = newdf[:, 0]   # select column 0, the stock price
import xgboost
import shap

# train an XGBoost model
# X, y = shap.datasets.california()
X = X.astype(float)
print(X.dtypes)
y = y.astype(float)
print(y.dtypes)
model = xgboost.XGBRegressor().fit(X, y)

# explain the model's predictions using SHAP
# (same syntax works for LightGBM, CatBoost, scikit-learn, transformers, Spark, etc.)
explainer = shap.Explainer(model)
shap_values = explainer(X)

# visualize the first prediction's explanation
shap.plots.waterfall(shap_values[0])