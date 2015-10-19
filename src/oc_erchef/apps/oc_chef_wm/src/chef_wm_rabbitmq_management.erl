-module(chef_wm_rabbitmq_management).

-ifdef(TEST).
-compile(export_all).
-endif.

-export([calc_ratio_and_percent/2,
         get_max_length/0,
         get_current_length/0,
         create_pool/0,
         delete_pool/0,
         get_pool_configs/0
        ]).


-define(POOLNAME, rabbitmq_management_service).

% exchange is /analytics, so encode the / as %2F
% NOTE: oc_httpc client is configured to prepend /api
-define(MAX_LENGTH_PATH, "/policies/%2Fanalytics/max_length").
-define(QUEUE_LENGTH_PATH, "/queues/%2Fanalytics").

%% oc_httpc pool functions --------------------------------------------
create_pool() ->
    Pools = get_pool_configs(),
    [oc_httpc:add_pool(PoolNameAtom, Config) || {PoolNameAtom, Config} <- Pools, Config /= []],
    ok.

delete_pool() ->
    Pools = get_pool_configs(),
    [ok = oc_httpc:delete_pool(PoolNameAtom) || {PoolNameAtom, _Config} <- Pools],
    ok.

get_pool_configs() ->
    Config = envy:get(oc_chef_wm, ?POOLNAME, [], any),
    [{?POOLNAME, Config}].



-spec calc_ratio_and_percent(integer(), integer()) -> {float(), float()}.
calc_ratio_and_percent(0, _MaxLength) ->
    {0.0, 0.0};
%% the max_length policy should ensure that CurrentLength <= MaxLength,
%% but return 100% full if this ever happens
calc_ratio_and_percent(CurrentLength, MaxLength) when CurrentLength >= MaxLength ->
    {1.0, 100.0};
calc_ratio_and_percent(CurrentLength, MaxLength) ->
    Ratio = CurrentLength / MaxLength,
    Pcnt = round(Ratio * 100.0),
    {Ratio, Pcnt}.

-spec rabbit_mgmt_server_request(string()) -> oc_httpc:response().
rabbit_mgmt_server_request(Path) ->
    oc_httpc:request(?POOLNAME, Path, [], get, []).


% make an http connection to the rabbitmq management console
% and return a integer value or undefined
-spec get_max_length() -> integer() | undefined.
get_max_length() ->
    MaxResult = rabbit_mgmt_server_request(?MAX_LENGTH_PATH),
    case MaxResult of
        {ok, "200", _, MaxLengthJson} ->
            parse_max_length_response(MaxLengthJson);
        {error, {conn_failed,_}} ->
            lager:info("Can't connect to RabbitMQ management console"),
            undefined;
        {ok, "404", _, _} ->
            lager:info("RabbitMQ max-length policy not set"),
            undefined;
        Resp ->
            lager:error("Unknown response from RabbitMQ management console: ~p", [Resp]),
            undefined

    end.

% make an http connection to the rabbitmq management console
% and return a integer value or undefined
-spec get_current_length() -> integer() | undefined.
get_current_length() ->
    CurrentResult = rabbit_mgmt_server_request(?QUEUE_LENGTH_PATH),
    case CurrentResult of
        {error, {conn_failed,_}} ->
            lager:info("Can't connect to RabbitMQ management console"),
            undefined;
        {ok, "200", _, CurrentStatusJson} ->
            parse_current_length_response(CurrentStatusJson);
        {ok, "404", _, _} ->
            lager:info("Queue not bound in /analytics exchange"),
            undefined;
        Resp ->
            lager:error("Unknown response from RabbitMQ management console: ~p", [Resp]),
            undefined
    end.

% NOTE: oc_httpc:responseBody() :: string() | {file, filename()}.
% reach into the JSON returned from the RabbitMQ management console
% and return a current length value, OR undefined if unavailable or
% unparseable. EJ was not convenient for parsing this data.
-spec parse_current_length_response(binary() | {file, oc_httpc:filename()}) -> integer() | undefined.
parse_current_length_response(Message) ->
    try
        CurrentJSON = jiffy:decode(Message),
        % make a proplists of each queue and it's current length
        QueueLengths =
            lists:map(fun (QueueStats) -> {QS} = QueueStats,
                                        {proplists:get_value(<<"name">>, QS),
                                        proplists:get_value(<<"messages">>, QS)}
                    end, CurrentJSON),
        % look for the alaska queue length
        parse_integer(proplists:get_value(<<"alaska">>, QueueLengths, undefined))
    catch
        Error:Rsn ->
            lager:error("Invalid RabbitMQ response while getting queue length ~p ~p",
                                 [Error, Rsn]),
            undefined
    end.

% NOTE: oc_httpc:responseBody() :: string() | {file, filename()}.
% reach into the JSON returned from the RabbitMQ management console
% and return the max_length value, OR undefined if unavailable or
% unparseable. EJ was not convenient for parsing this data.
-spec parse_max_length_response(binary() | {file, oc_httpc:filename()}) -> integer() | undefined.
parse_max_length_response(Message) ->
    try
        {MaxLengthPolicy} = jiffy:decode(Message),
        {Defs} = proplists:get_value(<<"definition">>, MaxLengthPolicy),
        parse_integer(proplists:get_value(<<"max-length">>, Defs, undefined))
    catch
        Error:Rsn->
            lager:error("Invalid RabbitMQ response while getting queue max length ~p ~p",
                        [Error, Rsn]),
            undefined
    end.


-spec parse_integer(any()) -> integer | undefined.
parse_integer(Val) when is_integer(Val) ->
    Val;
parse_integer(Val) when is_list(Val) ->
    case string:to_integer(Val) of
        {error, _Reason} -> undefined;
        {Int, _Rest} -> Int
    end;
parse_integer(_) -> undefined.

