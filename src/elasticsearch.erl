-module(elasticsearch).

-include("elasticsearch.hrl").

-ifdef(TEST).
-compile([export_all]).
-endif.

-export([
    start/0,
    transaction/1,
    create_index/1,
    create_index/2,
    create_index/3,
    delete_index/1,
    delete_index/2,
    create_mapping/3,
    path_exists/1,
    bulk_index/1,
    index/3,
    index/4,
    index/5,
    index/6,
    update/4,
    update/5,
    update/6,
    delete/3,
    delete/4,
    delete/5,
    search/3,
    search/4,
    search/5,
    scroll/4,
    scroll/5,
    count/3,
    count/4,
    count/5,
    test/0
]).

start() ->
    application:start(inets),
    application:start(poolboy),
    application:start(jsx),
    application:start(elasticsearch).

test() ->
    elasticsearch:start(),
    {ok,_} = create_index("test-index", {2, 2}),
    {ok, Bin} = file:read_file("test/test.json"),
    create_mapping("test-index", "Test", Bin),
    Docs = [
      {
        <<"test-index">>, <<"Test">>, <<"id1">>,
        [
          {<<"record_id">>, 1},{<<"name">>, <<"None">>}, {<<"customer_id">>, 1},
          {<<"create_date">>, <<"2016-09-14T10:20:09+00:00">>}]
      },
      {
        <<"test-index">>, <<"Test">>, <<"id2">>,
        [
          {<<"record_id">>, 2},{<<"name">>, <<"Any">>}, {<<"customer_id">>, 1},
          {<<"create_date">>, <<"2016-09-15T11:20:09+00:00">>}]
      },
      {
        <<"test-index">>, <<"Test">>, <<"id3">>,
        [
          {<<"record_id">>, 3},{<<"name">>, <<"Any">>}, {<<"customer_id">>, 1},
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
    ok = elasticsearch:scroll("test-index", "Test", Query, Fun, [{size, 1},{scroll, <<"2m">>}]),
    {ok,[{_,true}]} = delete_index("test-index"),
    ok.

transaction(Fun) ->
    poolboy:transaction(elasticsearch, Fun).

-spec bulk_index_header(binary(), binary(), binary(), list()) -> binary().
bulk_index_header(Index, Type, Id, HeaderInformation) ->
    IndexHeaderJson1 = [
        {<<"_index">>, Index}
        ,{<<"_type">>, Type}
        | HeaderInformation
    ],

    IndexHeaderJson2 = case Id =:= undefined of
                           true ->  IndexHeaderJson1;
                           false -> [ {<<"_id">>, Id} | IndexHeaderJson1]
                       end,

    jsx:encode([{<<"index">>, IndexHeaderJson2}]).

%% Documents is [ {Index, Type, Id, Json}, {Index, Type, Id, HeaderInformation, Json}... ]
bulk_index(IndexTypeIdJsonTup) ->
    JsonList = lists:map(fun
                         Parse({Index, Type, Id, Doc}) ->
                             Parse({Index, Type, Id, [], Doc});
                         Parse({Index, Type, Id, HeaderInformation, Doc}) ->
                             Header = bulk_index_header(Index, Type, Id, HeaderInformation),
                             [ Header, <<"\n">>, elasticsearch_worker:body_encode(Doc), <<"\n">> ]
                     end, IndexTypeIdJsonTup),
    elasticsearch_worker:do_request(
      post, ["_bulk"], iolist_to_binary(JsonList), []
    ).


create_mapping(Index, MappingName, Content) ->
    Path = string:join([Index, "_mapping", MappingName], "/"),
    case path_exists(Path) of
        {ok, true} -> {ok, mapping_exists};
        {ok, false} -> create_mapping(Path, Content)
    end.

create_mapping(Path, Content) ->
    Tokens = string:tokens(Path, "/"),
    poolboy:transaction(elasticsearch, fun (Worker) ->
        elasticsearch_worker:request(Worker, post, Tokens, Content, [])
    end).

delete_index(Name) ->
    case path_exists(Name) of
        {ok, true} -> delete_index(Name, true);
        {ok, false} -> {ok, not_found}
    end.

delete_index(Name, Force) when Force == true ->
    poolboy:transaction(elasticsearch, fun (Worker) ->
        elasticsearch_worker:request(Worker, delete, [Name], <<>>, [])
    end).


create_index(Name) ->
  case path_exists(Name) of
    {ok, true} -> {ok, index_exists};
    {ok, false} -> create_index(Name, [], [])
  end.

create_index(Name, {Shards, Replicas}) ->
  Settings = get_index_settings(Shards, Replicas),
  case path_exists(Name) of
    {ok, true} -> {ok, index_exists};
    {ok, false} -> create_index(Name, Settings, [])
  end.

create_index(Name, _Settings, _Mappings) ->
    Settings = case _Settings of
                 [] -> <<>>;
                 <<>> -> <<>>;
                 _ -> _Settings
               end,
    poolboy:transaction(elasticsearch, fun (Worker) ->
        elasticsearch_worker:request(Worker, put, [Name], Settings, _Mappings)
    end).

path_exists(Path) ->
    Tokens = string:tokens(Path, "/"),
    elasticsearch_worker:do_request(
      head, [Tokens], <<>>, []
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
    poolboy:transaction(elasticsearch, fun (Worker) ->
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
    poolboy:transaction(elasticsearch, fun (Worker) ->
        update(Worker, Index, Type, Id, Doc, Params)
    end).

update(Worker, Index, Type, Id, Doc, Params) ->
    elasticsearch_worker:request(Worker, post, [Index, Type, Id, <<"_update">>], Doc, Params).

delete(Index, Type, Id) ->
    delete(Index, Type, Id, []).

delete(Worker, Index, Type, Id) when is_pid(Worker) ->
    delete(Worker, Index, Type, Id, []);
delete(Index, Type, Id, Params) ->
    poolboy:transaction(elasticsearch, fun (Worker) ->
        delete(Worker, Index, Type, Id, Params)
    end).

delete(Worker, Index, Type, Id, Params) ->
    elasticsearch_worker:request(Worker, delete, [Index, Type, Id], <<>>, Params).

search(Index, Type, Query) ->
    search(Index, Type, Query, []).

search(Worker, Index, Type, Query) when is_pid(Worker) ->
    search(Worker, Index, Type, Query, []);
search(Index, Type, Query, Params) ->
    poolboy:transaction(elasticsearch, fun (Worker) ->
        search(Worker, Index, Type, Query, Params)
    end).

search(Worker, Index, Type, Query, Params) ->
    elasticsearch_worker:request(Worker, post, [Index, Type, <<"_search">>], Query, Params).

count(Index, Type, Query) ->
    search(Index, Type, Query, []).

count(Worker, Index, Type, Query) when is_pid(Worker) ->
    count(Worker, Index, Type, Query, []);
count(Index, Type, Query, Params) ->
    poolboy:transaction(elasticsearch, fun (Worker) ->
        count(Worker, Index, Type, Query, Params)
    end).

count(Worker, Index, Type, Query, Params) ->
    elasticsearch_worker:request(Worker, post, [Index, Type, <<"_count">>], Query, Params).

scroll(Index, Type, Query, Callback) ->
  scroll(Index, Type, Query, Callback, []).

scroll(Worker, Index, Type, Query, Callback) when is_pid(Worker) ->
  scroll(Worker, Index, Type, Query, Callback, []);
scroll(Index, Type, Query, Callback, Params) ->
  poolboy:transaction(elasticsearch, fun (Worker) ->
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
  {ok, _Results} = elasticsearch_worker:request(Worker, get, [<<"_search">>, <<"scroll">>],  <<"">>,
    [{scroll, <<"1m">>},{scroll_id, ScrollId}]),
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
    [{index, [{number_of_shards, Shards}, {number_of_replicas, Replicas}] }]
  }],
  elasticsearch_worker:body_encode(Settings).