.PHONY: b black run

SRC_FILES = ./scrape_mfa.py

VENV = ./python/bin/activate
REQUESTS = ./python/lib/python3.7/site-packages/requests
BLACK = ./python/lib/python3.7/site-packages/black

$(REQUESTS) $(BLACK): $(VENV)
	source $(VENV) && pip install -r requirements.txt

$(VENV):
	python3 -m venv python

run: $(REQUESTS) $(VENV)
	source $(VENV) && python scrape_mfa.py

b black: $(BLACK)
	source $(VENV) && black \
		--target-version  py37 \
		--line-length 79 \
		$(SRC_FILES)
