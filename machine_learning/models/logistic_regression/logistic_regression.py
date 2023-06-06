import joblib
from sklearn.calibration import CalibratedClassifierCV
from sklearn.linear_model import LogisticRegression


class LogisticRegressionModel:

    def __init__(self, random_state=42):
        self.model = LogisticRegression(random_state=random_state, verbose=1, n_jobs=-1)
        self.calibrated_model = None

    def train(self, X_train, y_train, best_params):
        self.model = LogisticRegression(random_state=42, **best_params, verbose=1, n_jobs=-1)

        self.model.fit(X_train, y_train)

        self.calibrated_model = CalibratedClassifierCV(self.model, method='sigmoid', cv=5)

        self.calibrated_model.fit(X_train, y_train)

    def predict(self, X_test):
        return self.calibrated_model.predict(X_test)

    def predict_proba(self, X_test):
        return self.calibrated_model.predict_proba(X_test)

    def save_model(self, filename):
        joblib.dump(self.calibrated_model, filename)