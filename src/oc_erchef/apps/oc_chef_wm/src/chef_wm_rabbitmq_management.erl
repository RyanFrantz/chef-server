-module(chef_wm_rabbitmq_management).

-ifdef(TEST).
-compile(export_all).
-endif.

-export([calc_ratio_and_percent/2,
         get_max_length/1,
         get_current_length/2,
         create_pool/0,
         delete_pool/0,
         get_pool_configs/0,
         check_current_queue_state/3,
         check_current_queue_length/4,
         sync_check_queue_at_capacity/2
        ]).


-define(POOLNAME, rabbitmq_management_service).
-define(LOG_THRESHOLD, 0.8).

-type max_length() :: integer().
-type current_length() :: integer().
-type queue_at_capacity() :: boolean.


% NOTE: oc_httpc client is configured to prepend /api

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

-spec mk_max_length_path(string()) -> string().
mk_max_length_path(Vhost) ->
    lists:flatten(io_lib:format("/policies/~s/max_length", [Vhost])).

-spec mk_current_length_path(string()) -> string().
mk_current_length_path(Vhost) ->
    lists:flatten(io_lib:format("/queues/~s", [Vhost])).

% make an http connection to the rabbitmq management console
% and return a integer value or undefined
-spec get_max_length(string()) -> integer() | undefined.
get_max_length(Vhost) ->
    MaxResult = rabbit_mgmt_server_request(mk_max_length_path(Vhost)),
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
-spec get_current_length(string(), binary()) -> integer() | undefined.
get_current_length(Vhost, Queue) ->
    CurrentResult = rabbit_mgmt_server_request(mk_current_length_path(Vhost)),
    case CurrentResult of
        {error, {conn_failed,_}} ->
            lager:info("Can't connect to RabbitMQ management console"),
            undefined;
        {ok, "200", _, CurrentStatusJson} ->
            parse_current_length_response(CurrentStatusJson, Queue);
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
-spec parse_current_length_response(binary() | {file, oc_httpc:filename()},
                                    binary()) -> integer() | undefined.
parse_current_length_response(Message, Queue) ->
    try
        CurrentJSON = jiffy:decode(Message),
        % make a proplists of each queue and it's current length
        QueueLengths =
            lists:map(fun (QueueStats) -> {QS} = QueueStats,
                                        {proplists:get_value(<<"name">>, QS),
                                        proplists:get_value(<<"messages">>, QS)}
                    end, CurrentJSON),
        % look for the alaska queue length
        parse_integer(proplists:get_value(Queue, QueueLengths, undefined))
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


-spec sync_check_queue_at_capacity(string(), binary()) -> {integer(), integer(), boolean()}.
sync_check_queue_at_capacity(Vhost, Queue) ->
    Result = check_current_queue_state(Vhost, Queue, 0),
    case Result of
      skipped  -> {0, 0, false};
      {MaxLength, reset_dropped_since_last_check} -> {MaxLength, 0, false};
      {MaxLength, N,QueueAtCapacity} -> {MaxLength, N, QueueAtCapacity}
    end.



-spec check_current_queue_state(string(), string(), integer()) ->
                                                skipped |
                                                {max_length(), reset_dropped_since_last_check} |
                                                {max_length(), current_length(), queue_at_capacity()}.
check_current_queue_state(Vhost, Queue, DroppedSinceLastCheck) ->
    case chef_wm_rabbitmq_management:get_max_length(Vhost) of
        undefined -> skipped;
                     % max length isn't configured, or something is broken
                     % don't continue.
        MaxLength ->
            lager:debug("Queue Monitor max length = ~p", [MaxLength]),
            check_current_queue_length(Vhost,
                                       Queue,
                                       MaxLength,
                                       DroppedSinceLastCheck)
    end.

-spec check_current_queue_length(string(), binary(), integer(), integer()) ->
                                                {max_length(), reset_dropped_since_last_check} |
                                                {max_length(), current_length(), queue_at_capacity()}.
check_current_queue_length(Vhost, Queue, MaxLength, DroppedSinceLastCheck) ->
    CurrentLength =
    chef_wm_rabbitmq_management:get_current_length(Vhost, Queue),
    case CurrentLength of
        undefined ->
            % a queue doesn't appear to be bound to the /analytics
            % exchange. The only thing we can do is reset the
            % dropped_since_last_check value to 0
            {MaxLength, reset_dropped_since_last_check};
        N ->
                lager:debug("Queue Monitor current length = ~p for VHost ~p and Queue ~p", [N, Vhost, Queue]),
                QueueAtCapacity = CurrentLength == MaxLength,
                {Ratio, Pcnt} = chef_wm_rabbitmq_management:calc_ratio_and_percent(CurrentLength, MaxLength),
                case Ratio >= ?LOG_THRESHOLD of
                    true ->
                        lager:warning("Queue Monitor has detected RabbitMQ for VHost ~p, queue ~p capacity at ~p%",
                                      [Vhost, Queue, Pcnt]);
                    false -> ok
                end,
                case QueueAtCapacity of
                    true ->
                        lager:warning("Queue Monitor has dropped ~p messages for VHost ~p, queue ~p since last check due to queue limit exceeded",
                                        [DroppedSinceLastCheck, Vhost, Queue]);
                    false -> ok
                end,
                % successfully checked max length and current length
                % update the state of the gen_server
                {MaxLength, N,QueueAtCapacity}
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

