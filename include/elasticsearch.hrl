-ifndef(elasticsearch_hrl).
-define(elasticsearch_hrl, 1).

-define(APPLICATION, elasticsearch).


-record(state, {
  base_url,
  http_options
}).

-define(ELASTICSEARCH_URL, elasticsearch_app:get_env(host, "http://localhost")).
-define(ELASTICSEARCH_PORT, elasticsearch_app:get_env(port, 9200)).

-define(PROFILE, elasticsearch).
-define(DEFAULT_HTTP_OPTIONS, [{timeout, 5000}]).
-define(HTTPC_OPTIONS, [{body_format, binary}, {full_result, false}]).

-endif.