-module(elasticsearch).

-include("elasticsearch.hrl").

-ifdef(TEST).
-compile([export_all]).
-endif.

-export([start/0, stop/0, transaction/1, test/0]).

%% Index operations
-export([create_index/1, create_index/2, create_index/3, delete_index/1, delete_index/2]).

%% Doc indexing
-export([bulk_index/1, index/3, index/4, index/5, index/6, update/4, update/5, update/6, delete/3,
  delete/4, delete/5]).

%% Search operations
-export([search/3, search/4, search/5, scroll/4, scroll/5, count/3, count/4, count/5]).

%% Misc
-export([create_mapping/3, path_exists/1, get_env/1, get_env/2, set_env/2]).

%% @doc Starts the required dependencies
start() ->
  {ok, Started} = application:ensure_all_started(?APPLICATION, permanent),
  utils:debug("started applications: ~p", [Started]),
  ok.

-spec stop() ->
  ok.
%% @doc stops the elasticsearch application
stop() ->
  application:stop(?APPLICATION).

%% @doc Executes some tests, please be aware that this will create
%% and delete test index in the cluster
test() ->
  elasticsearch:start(),
  {ok, _} = create_index("test-index", {2, 2}),
  {ok, Bin} = file:read_file("test/test.json"),
  create_mapping("test-index", "Test", Bin),
  Docs = [
    {
      <<"test-index">>, <<"Test">>, <<"id1">>,
      [
        {<<"record_id">>, 1}, {<<"name">>, <<"None">>}, {<<"customer_id">>, 1},
        {<<"create_date">>, <<"2016-09-14T10:20:09+00:00">>}]
    },
    {
      <<"test-index">>, <<"Test">>, <<"id2">>,
      [
        {<<"record_id">>, 2}, {<<"name">>, <<"Any">>}, {<<"customer_id">>, 1},
        {<<"create_date">>, <<"2016-09-15T11:20:09+00:00">>}]
    },
    {
      <<"test-index">>, <<"Test">>, <<"id3">>,
      [
        {<<"record_id">>, 3}, {<<"name">>, <<"Any">>}, {<<"customer_id">>, 1},
        {<<"create_date">>, <<"2016-09-15T12:20:09+00:00">>}]
    }
  ],
  {ok, _} = bulk_index(Docs),
  Query = [
    {
      query, [
      {
        bool, [
        {
          must, [{match, [{customer_id, 1}]}]
        },
        {
          filter, [
          {range, [
            {create_date, [
              {gt, <<"2016-09-14T14:20:09+00:00">>},
              {lt, <<"2016-09-15T14:20:09+00:00">>}
            ]}]}
        ]
        }
      ]
      }
    ]
    }
  ],
  Fun = fun(Iterator) ->
    io:format("Iterator: ~p~n", [Iterator]),
    ok
        end,
  ok = elasticsearch:scroll("test-index", "Test", Query, Fun, [{size, 1}, {scroll, <<"2m">>}]),
  {ok, [{_, true}]} = delete_index("test-index"),
  ok.

%% @private Executes a poolboy transaction
%% @param Fun Callback to be executed on poolboy transaction
transaction(Fun) ->
  poolboy:transaction(elasticsearch, Fun).

-spec bulk_index_header(binary(), binary(), binary(), list()) -> binary().
%% @private Builds a doc digest in elasticsearch bulk format
%% @param Index Index name
%% @param Type is the mapping name
%% @param Id Doc id
%% @param HeaderInformation List of document headers
bulk_index_header(Index, Type, Id, HeaderInformation) ->
  IndexHeaderJson1 = [
    {<<"_index">>, Index}
    , {<<"_type">>, Type}
    | HeaderInformation
  ],
  IndexHeaderJson2 = case Id =:= undefined of
                       true -> IndexHeaderJson1;
                       false -> [{<<"_id">>, Id} | IndexHeaderJson1]
                     end,

  jsx:encode([{<<"index">>, IndexHeaderJson2}]).

-spec bulk_index(Bulk :: list()) -> term().
%% @doc Performs a bulk indexing operation against the cluster, this operation doesn't use
%% poolboy to avoid gen_server timeouts, it is a direct request using http
%% @param Bulk is [ {Index, Type, Id, Json}, {Index, Type, Id, HeaderInformation, Json}... ]
bulk_index(Bulk) ->
  JsonList = lists:map(fun
                         Parse({Index, Type, Id, Doc}) ->
                           Parse({Index, Type, Id, [], Doc});
                         Parse({Index, Type, Id, HeaderInformation, Doc}) ->
                           Header = bulk_index_header(Index, Type, Id, HeaderInformation),
                           [Header, <<"\n">>, elasticsearch_worker:body_encode(Doc), <<"\n">>]
                       end, Bulk),
  elasticsearch_worker:do_request(
    post, ["_bulk"], iolist_to_binary(JsonList), []
  ).


-spec create_mapping(Index :: list(), MappingName :: list(), Content :: list()) -> term().
%% @doc Creates a mapping "type" in the cluster using the json provided and name
%% @param Index index name
%% @param MappingName name of the mapping to use
%% @param Content json string to define the mapping
create_mapping(Index, MappingName, Content) ->
  Path = string:join([Index, "_mapping", MappingName], "/"),
  case path_exists(Path) of
    {ok, true} -> {ok, mapping_exists};
    {ok, false} -> create_mapping(Path, Content)
  end.


-spec create_mapping(Path :: list(), Content :: list()) -> term().
%% @doc Creates a mapping "type" in the cluster using the json provided and name
%% @param Path full path of the mapping composed by index_name/mapping_name
%% @param Content json string to use as mapping
create_mapping(Path, Content) ->
  Tokens = string:tokens(Path, "/"),
  poolboy:transaction(elasticsearch, fun(Worker) ->
    elasticsearch_worker:request(Worker, post, Tokens, Content, [])
                                     end).

-spec delete_index(Name :: list()) -> term().
%% @doc Deletes an index by its given name
%% @param Name name if the index to delete
delete_index(Name) ->
  case path_exists(Name) of
    {ok, true} -> delete_index(Name, true);
    {ok, false} -> {ok, not_found}
  end.

-spec delete_index(Name :: list(), Force :: boolean()) -> term().
%% @doc Forces an index deletion
%% @param Name name if the index to delete
%% @param Force boolean
delete_index(Name, Force) when Force == true ->
  poolboy:transaction(elasticsearch, fun(Worker) ->
    elasticsearch_worker:request(Worker, delete, [Name], <<>>, [])
                                     end).

-spec create_index(Name :: list()) -> term().
%% @doc Creates an index
%% @param Name name if the index to create
create_index(Name) ->
  case path_exists(Name) of
    {ok, true} -> {ok, index_exists};
    {ok, false} -> create_index(Name, [], [])
  end.

-spec create_index(Name :: list(), {Shards :: integer(), Replicas :: integer()}) -> term().
%% @doc Creates an index using the given configuration
%% @param Name name if the index to create
%% @param Shards number fo shards
%% @param Replicas number fo replicas
create_index(Name, {Shards, Replicas}) ->
  Settings = get_index_settings(Shards, Replicas),
  case path_exists(Name) of
    {ok, true} -> {ok, index_exists};
    {ok, false} -> create_index(Name, Settings, [])
  end.

-spec create_index(Name :: list(), _Settings :: list(), _Mappings :: list()) -> term().
%% @doc Creates an index using the given configuration
%% @param Name name if the index to create
create_index(Name, _Settings, _Mappings) ->
  Settings = case _Settings of
               [] -> <<>>;
               <<>> -> <<>>;
               _ -> _Settings
             end,
  poolboy:transaction(elasticsearch, fun(Worker) ->
    elasticsearch_worker:request(Worker, put, [Name], Settings, _Mappings)
                                     end).

-spec path_exists(Path :: list()) -> term().
%% @doc Gets the given path from elasticsearch
%% @param Path list containing the different pieces from url
path_exists(Path) ->
  Tokens = string:tokens(Path, "/"),
  elasticsearch_worker:do_request(
    head, Tokens, <<>>, []
  ).

index(Index, Type, Doc) ->
  index(Index, Type, undefined, Doc, []).

index(Worker, Index, Type, Doc) when is_pid(Worker) ->
  index(Worker, Index, Type, undefined, Doc, []);
index(Index, Type, Id, Doc) when is_binary(Id) ->
  index(Index, Type, Id, Doc, []);
index(Index, Type, Doc, Params) ->
  index(Index, Type, undefined, Doc, Params).

index(Worker, Index, Type, Id, Doc) when is_pid(Worker) ->
  index(Worker, Index, Type, Id, Doc, []);
index(Index, Type, Id, Doc, Params) ->
  poolboy:transaction(elasticsearch, fun(Worker) ->
    index(Worker, Index, Type, Id, Doc, Params)
                                     end).

index(Worker, Index, Type, undefined, Doc, Params) ->
  elasticsearch_worker:request(Worker, post, [Index, Type], Doc, Params);
index(Worker, Index, Type, Id, Doc, Params) ->
  elasticsearch_worker:request(Worker, put, [Index, Type, Id], Doc, Params).

update(Index, Type, Id, Doc) when is_binary(Id) ->
  update(Index, Type, Id, Doc, []).

update(Worker, Index, Type, Id, Doc) when is_pid(Worker) ->
  update(Worker, Index, Type, Id, Doc, []);
update(Index, Type, Id, Doc, Params) ->
  poolboy:transaction(elasticsearch, fun(Worker) ->
    update(Worker, Index, Type, Id, Doc, Params)
                                     end).

update(Worker, Index, Type, Id, Doc, Params) ->
  elasticsearch_worker:request(Worker, post, [Index, Type, Id, <<"_update">>], Doc, Params).

delete(Index, Type, Id) ->
  delete(Index, Type, Id, []).

delete(Worker, Index, Type, Id) when is_pid(Worker) ->
  delete(Worker, Index, Type, Id, []);
delete(Index, Type, Id, Params) ->
  poolboy:transaction(elasticsearch, fun(Worker) ->
    delete(Worker, Index, Type, Id, Params)
                                     end).

delete(Worker, Index, Type, Id, Params) ->
  elasticsearch_worker:request(Worker, delete, [Index, Type, Id], <<>>, Params).

search(Index, Type, Query) ->
  search(Index, Type, Query, []).

search(Worker, Index, Type, Query) when is_pid(Worker) ->
  search(Worker, Index, Type, Query, []);
search(Index, Type, Query, Params) ->
  poolboy:transaction(elasticsearch, fun(Worker) ->
    search(Worker, Index, Type, Query, Params)
                                     end).

search(Worker, Index, Type, Query, Params) ->
  elasticsearch_worker:request(Worker, post, [Index, Type, <<"_search">>], Query, Params).

count(Index, Type, Query) ->
  search(Index, Type, Query, []).

count(Worker, Index, Type, Query) when is_pid(Worker) ->
  count(Worker, Index, Type, Query, []);
count(Index, Type, Query, Params) ->
  poolboy:transaction(elasticsearch, fun(Worker) ->
    count(Worker, Index, Type, Query, Params)
                                     end).

count(Worker, Index, Type, Query, Params) ->
  elasticsearch_worker:request(Worker, post, [Index, Type, <<"_count">>], Query, Params).

scroll(Index, Type, Query, Callback) ->
  scroll(Index, Type, Query, Callback, []).

scroll(Worker, Index, Type, Query, Callback) when is_pid(Worker) ->
  scroll(Worker, Index, Type, Query, Callback, []);
scroll(Index, Type, Query, Callback, Params) ->
  poolboy:transaction(elasticsearch, fun(Worker) ->
    scroll(Worker, Index, Type, Query, Callback, Params)
                                     end).
scroll(Worker, Index, Type, Query, Callback, Params) ->
  _Results = elasticsearch_worker:request(Worker, post, [Index, Type, <<"_search">>], Query, Params),
  scroll_results(Worker, _Results, Callback).


scroll_results(_Worker, [{scroll_id, undefined}, _, _, _], _Callback) ->
  false;
scroll_results(_Worker, [{scroll_id, _ScrollId}, {total, Total}, {count, Count}, _], _Callback)
  when Count >= Total ->
  ok;
scroll_results(Worker, [{scroll_id, ScrollId}, {total, _Total}, {count, Count}, _], Callback) ->
  {ok, _Results} = elasticsearch_worker:request(Worker, get, [<<"_search">>, <<"scroll">>], <<"">>,
    [{scroll, <<"1m">>}, {scroll_id, ScrollId}]),
  Iterator = get_iterator(_Results, Count),
  Callback(Iterator),
  scroll_results(Worker, Iterator, Callback);
scroll_results(Worker, {ok, _Results}, Callback) ->
  Iterator = get_iterator(_Results, 0),
  Callback(Iterator),
  scroll_results(Worker, Iterator, Callback);
scroll_results(_Worker, {error, _Any}, _Callback) ->
  {error, _Any}.

get_iterator(undefined, _CurrentCount) ->
  [
    {scroll_id, undefined},
    {total, 0},
    {count, 0},
    {results, []}
  ];
get_iterator(_Hits, CurrentCount) ->
  Results = proplists:get_value(<<"hits">>, _Hits),
  Hits = proplists:get_value(<<"hits">>, Results),
  ResultsCount = length(Hits) + CurrentCount,
  [
    {scroll_id, proplists:get_value(<<"_scroll_id">>, _Hits)},
    {total, proplists:get_value(<<"total">>, Results)},
    {count, ResultsCount},
    {results, Hits}
  ].

get_index_settings(Shards, Replicas) ->
  Settings = [{settings,
    [{index, [{number_of_shards, Shards}, {number_of_replicas, Replicas}]}]
  }],
  elasticsearch_worker:body_encode(Settings).


-spec get_env(atom()) -> term() | 'undefined'.
%%------------------------------------------------------------------------------
%% @doc get an environment variable's value (or undefined if it doesn't exist)
%%------------------------------------------------------------------------------
get_env(Key) ->
  get_env(Key, 'undefined').


-spec get_env(atom(), term()) -> term().
%%------------------------------------------------------------------------------
%% @doc get an environment variable's value (or Default if it doesn't exist)
%%------------------------------------------------------------------------------
get_env(Key, Default) ->
  case application:get_env(?APPLICATION, Key) of
    {ok, X} -> X;
    'undefined' -> Default
  end.


-spec set_env(atom(), any()) -> ok.
%%------------------------------------------------------------------------------
%% @doc set the environment variable's value
%%------------------------------------------------------------------------------
set_env(Key, Value) ->
  application:set_env(?APPLICATION, Key, Value).
