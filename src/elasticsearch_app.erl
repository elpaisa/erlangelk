-module(elasticsearch_app).

-behavior(application).

-include("elasticsearch.hrl").

-export([
    start/2,
    stop/1,
    get_env/2
]).

%% --------------------------------------------------
%% application callbacks
%% --------------------------------------------------

start(_StartType, _StartArgs) ->
    {ok, _Pid} = start_elasticsearch_httpc_profile(),
    PoolSize = get_env(pool_size, 2),
    PoolMaxOverflow = get_env(pool_max_overflow, 10),
    HttpOptions = get_env(http_options, []),
    elasticsearch_sup:start_link([?ELASTICSEARCH_URL, ?ELASTICSEARCH_PORT,
        PoolSize, PoolMaxOverflow, HttpOptions]
    ).

stop(_State) ->
    ok.

%% --------------------------------------------------
%% @private start inets with an elasticsearch profile
%% --------------------------------------------------
start_elasticsearch_httpc_profile() ->
    inets:start(httpc, [{profile, elasticsearch}]).

get_env(Key, Default) ->
    case application:get_env(elasticsearch, Key) of
        undefined -> Default;
        {ok, Val} -> Val
    end.
