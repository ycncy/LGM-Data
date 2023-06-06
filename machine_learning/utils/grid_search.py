from sklearn.model_selection import GridSearchCV


class GridSearch:
    def __init__(self, model, param_grid, cv=5, scoring='accuracy'):
        self.model = model
        self.param_grid = param_grid
        self.cv = cv
        self.scoring = scoring
        self.grid_search = None

    def search_best_params(self, X_train, y_train):
        self.grid_search = GridSearchCV(self.model, self.param_grid, cv=self.cv, scoring=self.scoring)

        self.grid_search.fit(X_train, y_train)

    def get_best_params(self):
        return self.grid_search.best_params_