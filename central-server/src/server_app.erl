-module(server_app).
-export([start/1, stop/1]).

start(Port) -> spawn(fun() -> server(Port) end).
stop(Server) -> Server ! stop.

server(Port) ->
    {ok, LSock} = gen_tcp:listen(Port, [binary, {active, once}, {packet, line},{reuseaddr, true}]),
    Room = spawn(fun()-> room(#{<<"jose">> => {}, <<"joao">> => {}},#{<<"Hi">> => spawn(fun() -> album(#{<<"users">> => #{<<"jose">> => null, <<"joao">> => null}, <<"images">> => #{<<"image">> => #{<<"hash">> =>
                                                                                                                              <<"123456789">>, <<"size">> =>
                                                                                                                              <<"53999">>, <<"users">> => #{<<"jose">> =>
                                                                                                                                              <<"5">>}}}}) end)}) end),
    Login = spawn(fun() -> login(#{<<"jose">> => {<<"jose">>,{}}, <<"joao">> => {<<"joao">>,{}}}) end),
    spawn(fun() -> acceptor(LSock, Room, Login) end),
    receive stop -> ok end.

acceptor(LSock, Room, Login) ->
    {ok, Sock} = gen_tcp:accept(LSock),
    io:format("Accepted connection ~p~n", [Sock]),
    spawn(fun() -> acceptor(LSock, Room, Login) end),
    user(Sock, {}, Login, Room, {}).

login(Users) ->
    receive
        % LOGIN
        {login, Pid, {Username, Password}} ->
            case maps:find(Username, Users) of
                error ->
                    Pid ! {login, nok},
                    login(Users);
                {ok, {Password, {}}} ->
                    Pid ! {login, ok, Username},
                    login(Users#{Username => {Password, Pid}});
                {ok, {Password,_Pid}} ->
                    _Pid ! {tcp_close_another_login, {}},
                    Pid ! {login, ok, Username},
                    login(Users#{Username => {Password, Pid}});
                {ok, _} ->
                    Pid ! {login, nok},
                    login(Users)
            end;
        % REGISTER
        {register, Pid, {Username, Password}} ->
            case maps:find(Username, Users) of
                error ->
                    Pid ! {register, ok, Username},
                    login(Users#{Username => {Password, Pid}});
                {ok, _} ->
                    Pid ! {register, nok},
                    login(Users)
            end;
        {logout, {Username, Pid}} ->
            case maps:find(Username, Users) of
                {ok, {Password, Pid}} ->
                    login(Users#{Username => {Password, {}}});
                {ok, _} ->
                    login(Users);
                error ->
                    login(Users)
            end
    end.


room(Users, Albums) ->
    Self = self(),

    receive
        {enter, {Username,Pid}} ->
            room(Users#{Username => Pid}, Albums);
        {leave, {Username,Pid}} ->
            case maps:find(Username, Users) of
                {ok, Pid} ->
                    room(Users#{Username => {}}, Albums);
                {ok, _} ->
                    room(Users, Albums);
                error ->
                    room(Users, Albums)
            end;
        {list, Pid} ->
            AlbumNames = maps:keys(Albums),
            % Concatenate the album names with commas
            ConcatenatedNames = lists:foldl(fun(Name, Acc) ->
                if
                    Acc == <<>> -> Name;
                    true -> <<Acc/binary, ",", Name/binary>>
                end
            end, <<>>, AlbumNames),
            case ConcatenatedNames of
                <<>> ->
                    Pid ! {line, <<"No albums found\n">>};
                _ ->
                    ConcatenatedNamesWithNewline = <<ConcatenatedNames/binary, "\n">>,
                    Pid ! {line, ConcatenatedNamesWithNewline}
                    % Add a newline character at the end
            end,
            room(Users, Albums);
        {createAlbum, {Username,Pid}, AlbumName} ->
            % Check if the album already exists
            case maps:find(AlbumName, Albums) of
                error ->
                    room(Users, Albums#{AlbumName => spawn(fun() -> album(#{<<"users">> => #{Username => null}, <<"images">> => #{}}) end)});
                {ok, _} ->
                    Pid ! {line, <<"Album already exists\n">>},
                    room(Users, Albums)
            end;
        {enterAlbum, {Username,Pid}, SelectedAlbumName, Router} ->
            case maps:find(SelectedAlbumName, Albums) of
                error ->
                    Pid ! {line, <<"Album not found\n">>},
                    room(Users, Albums);
                {ok, AlbumPid} ->
                    AlbumPid ! {enter, {Username,Pid}, Router},
                    room(Users, Albums)
            end
    end.

album(Metadata) ->
    UsersAlbum = maps:get(<<"users">>,Metadata),
    % Files = maps:get(<<"images">>,Metadata),
    receive
        {enter, {Username,Pid}, Router} ->
            case maps:find(Username, UsersAlbum) of
                error ->
                    Pid ! {line, <<"User does not have permission to enter album\n">>},
                    album(Metadata);
                {ok, _} ->
                    Pid ! {enterAlbum, self()},
                    [maps:get(<<"pid">>,Map) ! {line, << Router/binary, "\n">>} || Map <- lists:filter(fun(X) -> X =/= null end, maps:values(UsersAlbum))],
                    album(Metadata#{<<"users">> => UsersAlbum#{Username => #{<<"pid">> => Pid, <<"router">> => Router}}})
            end;
        {line, Data} ->
            [Pid ! {line, Data} || {Pid,_} <- lists:filter(fun(X) -> X =/= null end, maps:values(UsersAlbum))],
            album(Metadata);
        {leave, {Username,Pid}} ->
            case maps:find(Username, UsersAlbum) of
                {ok, null} ->
                    album(Metadata);
                {ok, Map} ->
                    case maps:find(<<"pid">>, Map) of
                        {ok, Pid} ->
                            album(Metadata#{<<"users">> => UsersAlbum#{Username => null}});
                        error ->
                            album(Metadata)
                    end;
                error ->
                    album(Metadata)
            end;
        {addUser, Pid, Username} ->
            case maps:find(Username, UsersAlbum) of
                error ->
                    Pid ! {line, <<"User added to Album\n">>},
                    album(Metadata#{<<"users">> => UsersAlbum#{Username => null}});
                {ok, _} ->
                    Pid ! {line, <<"User already in album\n">>},
                    album(Metadata)
            end;
        {removeUser, Pid, Username} ->
            case maps:find(Username, UsersAlbum) of
                error ->
                    Pid ! {line, <<"User not found\n">>},
                    album(Metadata);
                {ok, {}} ->
                    Pid ! {line, <<"User removed from album\n">>},
                    album(Metadata#{<<"users">> => maps:remove(Username, UsersAlbum)});
                {ok, _Pid} ->
                    Pid ! {line, <<"User removed from album\n">>},
                    % remove key from map
                    _Pid ! {removedFromAlbum, self()},
                    album(Metadata#{<<"users">> => maps:remove(Username, UsersAlbum)})
            end;
        {getMetadata, Pid} ->
            % #{Username => Pid},#{Filename => {Hash, #{Username => Rating(0-5)}}}
            % UsersAlbum keys to json like Usernames : [Username1, Username2]
            % use jsone:encode to convert {UsersAlbum, Files} to json
            % Json = json:encode([UsersAlbum, Files]),
            Json = jsone:encode(Metadata),
            Pid ! {line, << Json/binary , "\n">>},
            album(Metadata);
        {setMetadata, Pid, _Metadata} ->
            __Metadata = jsone:decode(_Metadata),
            io:format("~p~n", [__Metadata]),
            _UsersAlbum = maps:get(<<"users">>,__Metadata),
            __UsersAlbum = adjust_map(UsersAlbum, _UsersAlbum),
            _Files = maps:get(<<"images">>,__Metadata),
            
            ___Metadata = Metadata#{<<"users">> => __UsersAlbum, <<"images">> => _Files},
            Json = jsone:encode(___Metadata),
            Pid ! {line, << Json/binary , "\n">>},
            album(___Metadata)
    end.

% Function to adjust MapA based on MapB
adjust_map(MapA, MapB) ->
    % Remove users from MapA that are not in MapB
    FilteredMapA = maps:filter(fun(Key, _Value) -> maps:is_key(Key, MapB) end, MapA),

    % Find all users in MapB that are not in FilteredMapA
    NewUsersInB = maps:filter(fun(Key, _Value) -> not maps:is_key(Key, FilteredMapA) end, MapB),
    
    % Add NewUsersInB to FilteredMapA with value null
    AddToMapA = maps:map(fun(_, _Value) -> null end, NewUsersInB),
    maps:merge(FilteredMapA, AddToMapA).

trim_newline(Binary) ->
    % Check if Binary ends with a newline character
    case binary:match(Binary, <<"\n">>) of
        nomatch -> Binary;
        _ -> binary:part(Binary, 0, byte_size(Binary) - 1)
    end.

user(Sock, Username, LoginPid, PrimaryRoomPid, AlbumPid) ->
    Self = self(),
    User = {Username, Self},
    inet:setopts(Sock, [{active, once}]),
    receive
        {register, ok, _Username} ->
            gen_tcp:send(Sock, <<"registered\n">>),
            inet:setopts(Sock, [{active, once}]),
            PrimaryRoomPid ! {enter, {_Username, Self}},
            user(Sock, _Username, LoginPid, PrimaryRoomPid, AlbumPid);
        {register, nok} ->
            gen_tcp:send(Sock, <<"user already exists\n">>),
            inet:setopts(Sock, [{active, once}]),
            user(Sock, Username, LoginPid, PrimaryRoomPid, AlbumPid);
        {login, ok, _Username} ->
            gen_tcp:send(Sock, <<"logged in ", _Username/binary, "\n">>),
            inet:setopts(Sock, [{active, once}]),
            PrimaryRoomPid ! {enter, {_Username, Self}},
            user(Sock, _Username, LoginPid, PrimaryRoomPid, AlbumPid);
        {login, nok} ->
            gen_tcp:send(Sock, <<"wrong user or password\n">>),
            inet:setopts(Sock, [{active, once}]),
            user(Sock, Username, LoginPid, PrimaryRoomPid, AlbumPid);
        {enterAlbum, NewAlbumPid} ->
            gen_tcp:send(Sock, <<"entered album\n">>),
            inet:setopts(Sock, [{active, once}]),
            PrimaryRoomPid ! {leave, User},
            user(Sock, Username, LoginPid, PrimaryRoomPid, NewAlbumPid);
        {removedFromAlbum, _} ->
            gen_tcp:send(Sock, <<"removed from album\n">>),
            inet:setopts(Sock, [{active, once}]),
            PrimaryRoomPid ! {enter, User},
            user(Sock, Username, LoginPid, PrimaryRoomPid, {});
        {line, {{_Username, Self}, Data}} ->
            io:format("~p sended ~p~n", [Self,Data]),
            inet:setopts(Sock, [{active, once}]),
            gen_tcp:send(Sock, Data),
            user(Sock, Username, LoginPid, PrimaryRoomPid, AlbumPid);
        {line, {{_Username, _}, Data}} ->
            io:format("~p received ~p~n", [Self,Data]),
            gen_tcp:send(Sock, Data),
            inet:setopts(Sock, [{active, once}]),
            user(Sock, Username, LoginPid, PrimaryRoomPid, AlbumPid);
        {line, Data} ->
            io:format("~p received ~p~n", [Self,Data]),
            gen_tcp:send(Sock, Data),
            inet:setopts(Sock, [{active, once}]),
            user(Sock, Username, LoginPid, PrimaryRoomPid, AlbumPid);
        {tcp, _, Data} ->
            io:format("~p received command ~p~n", [Self,Data]),

            case Username of
                {} ->
                    case Data of
                        <<"/help\n", _/binary>> ->
                            gen_tcp:send(Sock, <<"Commands: /register, /login, /quit\n">>),
                            inet:setopts(Sock, [{active, once}]),
                            user(Sock, Username, LoginPid, PrimaryRoomPid, AlbumPid);
                        <<"/quit\n", _/binary>> ->
                            ok;
                        <<"/register ", UsernamePassword/binary>> ->
                            [_Username, Password] = binary:split(UsernamePassword, <<" ">>),
                            _Password = trim_newline(Password),
                            LoginPid ! {register, Self, {_Username, _Password}},
                            inet:setopts(Sock, [{active, once}]),
                            user(Sock, Username, LoginPid, PrimaryRoomPid, AlbumPid);
                        <<"/login ", UsernamePassword/binary>> ->
                            [_Username, Password] = binary:split(UsernamePassword, <<" ">>),
                            _Password = trim_newline(Password),
                            LoginPid ! {login, Self, {_Username, _Password}},
                            inet:setopts(Sock, [{active, once}]),
                            user(Sock, Username, LoginPid, PrimaryRoomPid, AlbumPid);
                        _ ->
                            gen_tcp:send(Sock, <<"unknown command\n">>),
                            inet:setopts(Sock, [{active, once}]),
                            user(Sock, Username, LoginPid, PrimaryRoomPid, AlbumPid)
                    end;
                _ ->
                    
                    case AlbumPid of
                        {} ->
                            case Data of
                                <<"/help\n", _/binary>> ->
                                    gen_tcp:send(Sock, <<"Commands: /quit, /send, /list, /createAlbum, /enterAlbum, /logout\n">>),
                                    inet:setopts(Sock, [{active, once}]),
                                    user(Sock, Username, LoginPid, PrimaryRoomPid, AlbumPid);
                                <<"/quit\n", _/binary>> ->
                                    PrimaryRoomPid ! {leave, User},
                                    LoginPid ! {logout, User},
                                    user(Sock, {}, LoginPid, PrimaryRoomPid, AlbumPid);
                                <<"/send ", Rest/binary>> ->
                                    io:format("~p sending ~p~n", [Self, Rest]),
                                    PrimaryRoomPid ! {line, {User, Rest}},
                                    user(Sock, Username, LoginPid, PrimaryRoomPid, AlbumPid);
                                <<"/list\n", _/binary>> ->
                                    PrimaryRoomPid ! {list, Self},
                                    user(Sock, Username, LoginPid, PrimaryRoomPid, AlbumPid);
                                <<"/createAlbum ", Nome/binary>> ->
                                    _Nome = trim_newline(Nome),
                                    PrimaryRoomPid ! {createAlbum, User, _Nome},
                                    inet:setopts(Sock, [{active, once}]),
                                    user(Sock, Username, LoginPid, PrimaryRoomPid, AlbumPid);
                                <<"/enterAlbum ", _Data/binary>> ->
                                    [Nome, Router] = binary:split(_Data, <<" ">>),
                                    _Router = trim_newline(Router),
                                    _Nome = trim_newline(Nome),
                                    PrimaryRoomPid ! {enterAlbum, {Username, Self}, _Nome, _Router},
                                    inet:setopts(Sock, [{active, once}]),
                                    user(Sock, Username, LoginPid, PrimaryRoomPid, AlbumPid);
                                <<"/logout\n", _/binary>> ->
                                    PrimaryRoomPid ! {leave, User},
                                    LoginPid ! {logout, User},
                                    user(Sock, {}, LoginPid, PrimaryRoomPid, AlbumPid);
                                _ ->
                                    gen_tcp:send(Sock, <<"unknown command\n">>),
                                    inet:setopts(Sock, [{active, once}]),
                                    user(Sock, Username, LoginPid, PrimaryRoomPid, AlbumPid)
                            end;
                        _ ->
                            case Data of
                                <<"/help\n", _/binary>> ->
                                    gen_tcp:send(Sock, <<"Commands: /quit, /send, /addUser, /removeUser, /logout\n">>),
                                    inet:setopts(Sock, [{active, once}]),
                                    user(Sock, Username, LoginPid, PrimaryRoomPid, AlbumPid);
                                <<"/quit\n", _/binary>> ->
                                    AlbumPid ! {leave, User},
                                    PrimaryRoomPid ! {enter, User},
                                    user(Sock, Username, LoginPid, PrimaryRoomPid, {});
                                <<"/send ", Rest/binary>> ->
                                    io:format("~p sending ~p~n", [Self, Rest]),
                                    AlbumPid ! {line, {User, Rest}},
                                    user(Sock, Username, LoginPid, PrimaryRoomPid, AlbumPid);
                                <<"/logout\n", _/binary>> ->
                                    AlbumPid ! {leave, User},
                                    LoginPid ! {logout, User},
                                    user(Sock, {}, LoginPid, PrimaryRoomPid, {});
                                % <<"/addUser ", _Username/binary>> ->
                                %     io:format("~p adding user ~p~n", [Username, _Username]),
                                %     __User = trim_newline(_Username),
                                %     AlbumPid ! {addUser, Self, __User},
                                %     inet:setopts(Sock, [{active, once}]),
                                %     user(Sock, Username, LoginPid, PrimaryRoomPid, AlbumPid);
                                % <<"/removeUser ", _Username/binary>> ->
                                %     io:format("~p removing user ~p~n", [Username, _Username]),
                                %     __Username = trim_newline(_Username),
                                %     AlbumPid ! {removeUser, Self, __Username},
                                %     inet:setopts(Sock, [{active, once}]),
                                %     user(Sock, Username, LoginPid, PrimaryRoomPid, AlbumPid);
                                <<"/getMetadata\n", _/binary>> ->
                                    AlbumPid ! {getMetadata, Self},
                                    inet:setopts(Sock, [{active, once}]),
                                    user(Sock, Username, LoginPid, PrimaryRoomPid, AlbumPid);
                                <<"/setMetadata ", Metadata/binary>> ->
                                    AlbumPid ! {setMetadata, Self, Metadata},
                                    inet:setopts(Sock, [{active, once}]),
                                    user(Sock, Username, LoginPid, PrimaryRoomPid, AlbumPid);
                                _ ->
                                    gen_tcp:send(Sock, <<"unknown command\n">>),
                                    inet:setopts(Sock, [{active, once}]),
                                    user(Sock, Username, LoginPid, PrimaryRoomPid, AlbumPid)
                            end
                    end
            end;
        {tcp_close_another_login, _} ->
            gen_tcp:send(Sock, <<"Another user logged in with the same credentials\n">>),
            case Username of
                {} ->
                    ok;
                _ ->
                    case AlbumPid of
                        {} ->
                            PrimaryRoomPid ! {leave, User};
                        _ ->
                            AlbumPid ! {leave, User}
                    end,
                    LoginPid ! {logout, User}
            end;
        {tcp_closed, _} ->
            io:format("Connection closed ~p~n", [Sock]),
            case Username of
                {} ->
                    ok;
                _ ->
                    case AlbumPid of
                        {} ->
                            PrimaryRoomPid ! {leave, User};
                        _ ->
                            AlbumPid ! {leave, User}
                    end,
                    LoginPid ! {logout, User}
            end;
        {tcp_error, _, _} ->
            io:format("Connection error ~p~n", [Sock]),
            case Username of
                {} ->
                    ok;
                _ ->
                    case AlbumPid of
                        {} ->
                            PrimaryRoomPid ! {leave, User};
                        _ ->
                            AlbumPid ! {leave, User}
                    end,
                    LoginPid ! {logout, User}
            end
    end.
