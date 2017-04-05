%%%-------------------------------------------------------------------
%%% @author johnleytondiaz
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 05. Apr 2017 12:03 PM
%%%-------------------------------------------------------------------
-module(elasticsearch_utils).
-author("johnleytondiaz").

%% API
-export([escape_uri/1]).

% Utility function to convert a 'form' of name-value pairs into a URL encoded
% content string.

escape_uri(S) when is_list(S) ->
  escape_uri(unicode:characters_to_binary(S));
escape_uri(<<C:8, Cs/binary>>) when C >= $a, C =< $z ->
  [C] ++ escape_uri(Cs);
escape_uri(<<C:8, Cs/binary>>) when C >= $A, C =< $Z ->
  [C] ++ escape_uri(Cs);
escape_uri(<<C:8, Cs/binary>>) when C >= $0, C =< $9 ->
  [C] ++ escape_uri(Cs);
escape_uri(<<C:8, Cs/binary>>) when C == $. ->
  [C] ++ escape_uri(Cs);
escape_uri(<<C:8, Cs/binary>>) when C == $- ->
  [C] ++ escape_uri(Cs);
escape_uri(<<C:8, Cs/binary>>) when C == $_ ->
  [C] ++ escape_uri(Cs);
escape_uri(<<C:8, Cs/binary>>) ->
  escape_byte(C) ++ escape_uri(Cs);
escape_uri(<<>>) ->
  "".

escape_byte(C) ->
  "%" ++ hex_octet(C).

hex_octet(N) when N =< 9 ->
  [$0 + N];
hex_octet(N) when N > 15 ->
  hex_octet(N bsr 4) ++ hex_octet(N band 15);
hex_octet(N) ->
  [N - 10 + $a].