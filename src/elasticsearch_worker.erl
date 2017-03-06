-module(elasticsearch_worker).

-behavior(gen_server).

-export([
    start_link/1,
    request/5,
    init/1,
    handle_call/3,
    body_encode/1,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-record(state, {
    base_url,
    http_options
}).

-define(PROFILE, elasticsearch).
-define(DEFAULT_HTTP_OPTIONS, [{timeout, 5000}]).
-define(HTTPC_OPTIONS, [{body_format, binary}, {full_result, false}]).

%%
%% API.
%%
start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

request(Worker, Method, Path, Body, Params) ->
    gen_server:call(Worker, {Method, Path, Body, Params}).

%%
%% gen_server
%%
init([Host, Port, HttpOptions]) ->
    BaseUrl = lists:concat([Host, ":", to_list(Port), "/"]),
    State = #state{
        base_url = BaseUrl,
        http_options = lists:ukeymerge(1, lists:usort(HttpOptions), ?DEFAULT_HTTP_OPTIONS)
    },
    {ok, State}.

handle_call({Method, Path, Body0, Params0}, _From, #state{ base_url = BaseUrl, http_options = HttpOptions } = State) ->
    URLPath = BaseUrl ++ string:join([escape(to_list(P)) || P <- Path], "/"),
    Body = case jsx:is_json(Body0) of
        true -> Body0;
        false -> body_encode(Body0)
    end,
    Params = string:join([string:join([to_list(Key), to_list(Value)], "=") || {Key, Value} <- Params0], "&"),
    URL = if length(Params) > 0 -> lists:concat([URLPath, "?", Params]);
             true               -> URLPath
    end,
    Headers = [{"Content-Length", to_list(erlang:iolist_size(Body))}],
    Request = case Method of
        delete ->
            {URL, Headers};
        head ->
            {URL, Headers};
        _      ->
            {URL, Headers, "application/json", to_list(Body)}
    end,
    Reply = case httpc:request(Method, Request, HttpOptions, ?HTTPC_OPTIONS, ?PROFILE) of
        {ok, {Status, _}} when Status == 200, Method == head ->
            {ok, true};
        {ok, {Status, RespBody}} when Status == 200; Status == 201 ->
            {ok, search_result(RespBody)};
        {ok, {Status, _}} when Status == 404, Method == head ->
            {ok, false};
        {ok, {_Status, RespBodyFail}} ->
            {error, RespBodyFail};
        {error, Reason} ->
            {error, Reason}
    end,
    {reply, Reply, State};
handle_call(_Request, _From, State) ->
    {reply, ignored, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


body_encode(Bin) when is_binary(Bin) -> Bin;
body_encode(Doc) when is_list(Doc); is_tuple(Doc); is_map(Doc) -> jsx:encode(Doc).

%%
%% private
%%
search_result(Body) ->
    Result = jsx:decode(Body),
    Result.

to_list(List) when is_list(List) -> List;
to_list(Binary) when is_binary(Binary) -> binary_to_list(Binary);
to_list(Integer) when is_integer(Integer) -> integer_to_list(Integer);
to_list(Atom) when is_atom(Atom) -> atom_to_list(Atom).

escape(String) ->
    edoc_lib:escape_uri(String).
