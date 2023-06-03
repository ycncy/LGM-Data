from sklearn.calibration import CalibratedClassifierCV
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import GridSearchCV


class RandomForestModel:
    def __init__(self):
        self.model = RandomForestClassifier()

    def set_params(self, **params):
        self.model.set_params(**params)

    def get_params(self):
        return self.model.get_params()

    def find_best_params(self, X_train, y_train, param_grid):
        grid_search = GridSearchCV(CalibratedClassifierCV(self.model, cv=5), param_grid, cv=5)

        grid_search.fit(X_train, y_train)

        return grid_search.best_params_

    def fit_with_best_params(self, X_train, y_train, best_params):
        self.set_params(**best_params)

        self.model.fit(X_train, y_train)

    def predict(self, X_test):
        return self.model.predict(X_test)

    def predict_proba(self, X_test):
        return self.model.predict_proba(X_test)

    def score(self, X_train, y_train):
        return self.model.score(X_train, y_train)