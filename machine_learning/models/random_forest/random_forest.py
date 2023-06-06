from sklearn.calibration import CalibratedClassifierCV
from sklearn.ensemble import RandomForestClassifier


class RandomForestModel:
    def __init__(self, random_state=42, n_jobs=-1):
        self.model = RandomForestClassifier(random_state=random_state, n_jobs=n_jobs)
        self.calibrated_model = None

    def train(self, X_train, y_train):
        self.model.fit(X_train, y_train)

        self.calibrated_model = CalibratedClassifierCV(self.model, method='sigmoid', cv=5, n_jobs=-1)

        self.calibrated_model.fit(X_train, y_train)

    def predict(self, X_test):
        return self.calibrated_model.predict(X_test)

    def predict_proba(self, X_test):
        return self.calibrated_model.predict_proba(X_test)
