from sklearn.ensemble import GradientBoostingClassifier
from sklearn.model_selection import cross_val_score


class GradientBoostingModel:
    def __init__(self, best_params):
        self.model = GradientBoostingClassifier(**best_params)

    def train(self, x_train, y_train):
        self.model.fit(x_train, y_train)

    def predict_proba(self, x_test):
        return self.model.predict_proba(x_test)

    def cross_val_score(self, x_train, y_train, cv):
        return cross_val_score(self.model, x_train, y_train, cv=cv, n_jobs=-1)

    def mean_cross_val_score(self, x_train, y_train, cv):
        scores = self.cross_val_score(x_train, y_train, cv)

        return scores.mean()