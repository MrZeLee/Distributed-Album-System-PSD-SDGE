-module(server).
-export([start/1, stop/1]).

start(Port) -> spawn(fun() -> server(Port) end).
stop(Server) -> Server ! stop.

server(Port) ->
    {ok, LSock} = gen_tcp:listen(Port, [binary, {active, once}, {packet, line},{reuseaddr, true}]),
    Room = spawn(fun()-> room(#{},#{}) end),
    Login = spawn(fun() -> login(#{}) end),
    spawn(fun() -> acceptor(LSock, Room, Login) end),
    receive stop -> ok end.

acceptor(LSock, Room, Login) ->
    {ok, Sock} = gen_tcp:accept(LSock),
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
        {line, Data} ->
            [Pid ! {line, Data} || Pid <- lists:filter(fun(X) -> X =/= {} end, maps:values(Users))],
            room(Users, Albums);
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
                    room(Users, Albums#{AlbumName => spawn(fun() -> album(#{Username => {}}, Self, #{}) end)});
                {ok, _} ->
                    Pid ! {line, <<"Album already exists\n">>},
                    room(Users, Albums)
            end;
        {enterAlbum, {Username,Pid}, SelectedAlbumName} ->
            case maps:find(SelectedAlbumName, Albums) of
                error ->
                    Pid ! {line, <<"Album not found\n">>},
                    room(Users, Albums);
                {ok, AlbumPid} ->
                    AlbumPid ! {enter, {Username,Pid}},
                    room(Users, Albums)
            end;
        {addUserToAlbum, Pid, Username, AlbumPid} ->

            case maps:find(Username, Users) of
                error ->
                    Pid ! {line, <<"User not found\n">>},
                    room(Users, Albums);
                {ok, _} ->
                    AlbumPid ! {addUserHelper, Pid, Username},
                    room(Users, Albums)
            end
    end.

album(UsersAlbum, PrimaryRoom, Files) ->
    receive
        {enter, {User,Pid}} ->
            case maps:find(User, UsersAlbum) of
                error ->
                    Pid ! {line, <<"User does not have permission to enter album\n">>},
                    album(UsersAlbum, PrimaryRoom, Files);
                {ok, {}} ->
                    Pid ! {enterAlbum, self()},
                    album(UsersAlbum#{User => Pid}, PrimaryRoom, Files);
                {ok, _Pid} ->
                    % close previous connection
                    Pid ! {enterAlbum, self()},
                    album(UsersAlbum#{User => Pid}, PrimaryRoom, Files)
            end;
        {line, Data} ->
            [Pid ! {line, Data} || Pid <- lists:filter(fun(X) -> X =/= {} end, maps:values(UsersAlbum))],
            album(UsersAlbum, PrimaryRoom, Files);
        {leave, {Username,Pid}} ->
            case maps:find(Username, UsersAlbum) of
                {ok, Pid} ->
                    album(UsersAlbum#{Username => {}}, PrimaryRoom, Files);
                {ok, _Pid} ->
                    album(UsersAlbum, PrimaryRoom, Files);
                error ->
                    album(UsersAlbum, PrimaryRoom, Files)
            end;
        {addUser, Pid, User} ->
            PrimaryRoom ! {addUserToAlbum, Pid, User, self()},
            album(UsersAlbum, PrimaryRoom, Files);
        {addUserHelper, Pid, Username} ->
            case maps:find(Username, UsersAlbum) of
                error ->
                    Pid ! {line, <<"User added to Album\n">>},
                    album(UsersAlbum#{Username => {}} ,PrimaryRoom, Files);
                {ok, _} ->
                    Pid ! {line, <<"User already in album\n">>},
                    album(UsersAlbum, PrimaryRoom, Files)
            end
    end.

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
            gen_tcp:send(Sock, <<"logged in\n">>),
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
        {line, {{_Username, Self}, Data}} ->
            io:format("~p sended ~p~n", [Self,Data]),
            inet:setopts(Sock, [{active, once}]),
            Concatenated = <<_Username/binary, ": ", Data/binary>>,
            gen_tcp:send(Sock, Concatenated),
            user(Sock, Username, LoginPid, PrimaryRoomPid, AlbumPid);
        {line, {{_Username, _}, Data}} ->
            io:format("~p received ~p~n", [Self,Data]),
            Concatenated = <<_Username/binary, ": ", Data/binary>>,
            gen_tcp:send(Sock, Concatenated),
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
                                <<"/enterAlbum ", Nome/binary>> ->
                                    _Nome = trim_newline(Nome),
                                    PrimaryRoomPid ! {enterAlbum, {Username, Self}, _Nome},
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
                                    gen_tcp:send(Sock, <<"Commands: /quit, /send, /addUser, /logout\n">>),
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
                                <<"/addUser ", _Username/binary>> ->
                                    io:format("~p adding user ~p~n", [Username, _Username]),
                                    __User = trim_newline(_Username),
                                    AlbumPid ! {addUser, Self, __User},
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

