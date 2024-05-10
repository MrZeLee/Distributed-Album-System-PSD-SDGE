-module(server).
-export([start/1, stop/1]).

start(Port) -> spawn(fun() -> server(Port) end).
stop(Server) -> Server ! stop.

server(Port) ->
    {ok, LSock} = gen_tcp:listen(Port, [binary, {active, once}, {packet, line},{reuseaddr, true}]),
    Room = spawn(fun()-> room([],[]) end),
    Login = spawn(fun() -> login([]) end),
    spawn(fun() -> acceptor(LSock, Room, Login) end),
    receive stop -> ok end.

acceptor(LSock, Room, Login) ->
    {ok, Sock} = gen_tcp:accept(LSock),
    spawn(fun() -> acceptor(LSock, Room, Login) end),
    user(Sock, {}, Login, Room, {}).

login(Users) ->
    receive
        {login, Pid, {User, Password}} ->
            case lists:member({User, Password}, Users) of
                true ->
                    Pid ! {login, ok, User},
                    login(Users);
                false ->
                    Pid ! {login, nok},
                    login(Users)
            end;
        {register, Pid, {User, Password}} ->
            case lists:keyfind(User, 1, Users) of
                false ->
                    Pid ! {register, ok, User},
                    login([{User, Password} | Users]);
                {User, Password} ->
                    Pid ! {register, nok},
                    login(Users)
            end
    end.


room(Pids, Albums) ->
    Self = self(),
    io:format("room ~p~n", [Self]),

    receive
        {enter, Pid} ->
            io:format("~p user entered room~n", [Pid]),
            room([Pid | Pids], Albums);
        {line, Data} = Msg ->
            io:format("room:line, ~p~n", [Data]),
            [Pid ! Msg || Pid <- Pids],
            room(Pids, Albums);
        {leave, Pid} ->
            io:format("room-user left~n", []),
            room(Pids -- [Pid], Albums);
        {list, Pid, User} ->
            AlbumNames = [Nome || {Nome, Users, _} <- Albums, lists:member(User, Users)],
            % Concatenate the album names with commas
            ConcatenatedNames = lists:foldl(fun(Name, Acc) ->
                if
                    Acc == <<>> -> Name;
                    true -> <<Acc/binary, ",", Name/binary>>
                end
            end, <<>>, AlbumNames),
            io:format("chat-list ~p~n", [Albums]),
            ConcatenatedNamesWithNewline = <<ConcatenatedNames/binary, "\n">>,
            Pid ! {list, ConcatenatedNamesWithNewline},
            room(Pids, Albums);
        {createAlbum, Pid, User, Nome} ->
            % Check if the album already exists
            case lists:member(Nome, [Name || {Name, _, _} <- Albums]) of
                true ->
                    io:format("room-album already exists~n", []),
                    Pid ! {line, <<"Album already exists\n">>},
                    room(Pids, Albums);
                false ->
                    io:format("room-creating album~n", []),
                    % Album = Nome + Lista de Pids autorizados + Pid do Album
                    Album = {Nome, [User], spawn(fun() -> chat(Pid, [], Self) end)},
                    io:format("room-Album ~p~n", [Album]),
                    room(Pids, [Album | Albums])
            end;
        {enterAlbum, Pid, User, SelectedNome} ->
            % Finding the album by name using list comprehension
            io:format("room-Albums ~p~n", [Albums]),
            [SelectedAlbum] = [{Nome, _Users, _Pid} || {Nome, _Users, _Pid} <- Albums, Nome == SelectedNome],
            io:format("room-SelectedAlbum ~p~n", [SelectedAlbum]),
            {_, _Users, _Pid} = SelectedAlbum,
            %% Check if user is in _Users
            case lists:member(User, _Users) of
                true ->
                    Pid ! {enterAlbum, _Pid},
                    _Pid ! {enter, Pid},
                    room(Pids -- [Pid], Albums);
                false ->
                    Pid ! {line, <<"You don't have permission to enter the Album\n">>},
                    room(Pids, Albums)
            end;
        {addUser, Pid, User, AlbumPid} ->
            io:format("room-addUser ~p~n", [Pid]),
            case lists:keyfind(AlbumPid, 3, Albums) of
                {Nome, Users, AlbumPid} ->
                    io:format("Album ~p~n", [{Nome, Users, AlbumPid}]),
                    NewUsers = [User | Users],
                    NewAlbum = {Nome, NewUsers, AlbumPid},
                    io:format("NewAlbum ~p~n", [NewAlbum]),
                    NewAlbums = [Album || {ExistingNome, _, _} = Album <- Albums, ExistingNome =/= Nome] ++ [NewAlbum],
                    room(Pids, NewAlbums);
                false ->
                    Pid ! {line, <<"Album not found\n">>},
                    room(Pids, Albums)
            end
    end.

chat(Admin, Pids, Room) ->
    receive
        {enter, Pid} ->
            io:format("chat-user entered~n", []),
            chat(Admin, [Pid | Pids], Room);
        {line, Data} = Msg ->
            io:format("Album ~p:line, ~p~n", [self(),Data]),
            [Pid ! Msg || Pid <- Pids],
            chat(Admin, Pids, Room);
        {leave, Pid} ->
            io:format("chat-user left~n", []),
            chat(Admin, Pids -- [Pid], Room);
        {addUser, Pid, User} ->
            case Pid == Admin of
                true ->
                    io:format("adding user ~p~n", [User]),
                    io:format("Room ~p~n", [Room]),
                    Room ! {addUser, Pid, User, self()},
                    chat(Admin, Pids, Room);
                false ->
                    io:format("chat-only admin can add users~n", []),
                    Pid ! {line, <<"Only admin can add users\n">>},
                    chat(Admin, Pids, Room)
            end
    end.

trim_newline(Binary) ->
    % Check if Binary ends with a newline character
    case binary:match(Binary, <<"\n">>) of
        nomatch -> Binary;
        _ -> binary:part(Binary, 0, byte_size(Binary) - 1)
    end.

% %% Convert a single PID to binary string
% pid_to_binary(Pid) ->
%     PidString = erlang:pid_to_list(Pid),
%     list_to_binary(PidString).
%
% %% Convert a list of PIDs to a concatenated binary string
% pids_to_binary([]) ->
%     <<>>;
% pids_to_binary([Pid | Rest]) ->
%     PidBin = pid_to_binary(Pid),
%     RestBin = pids_to_binary(Rest),
%     <<PidBin/binary, ", ", RestBin/binary>>.

user(Sock, User, Login, Room, Album) ->
    Self = self(),
    inet:setopts(Sock, [{active, once}]),
    receive
        {enterAlbum, NewAlbum} ->
            user(Sock, User, Login, Room, NewAlbum);
        {line, {Self, _User, Data}} ->
            io:format("~p sended ~p~n", [Self,Data]),
            inet:setopts(Sock, [{active, once}]),
            Concatenated = <<_User/binary, ": ", Data/binary>>,
            gen_tcp:send(Sock, Concatenated),
            user(Sock, User, Login, Room, Album);
        {line, {_, _User, Data}} ->
            io:format("~p received ~p~n", [Self,Data]),
            Concatenated = <<_User/binary, ": ", Data/binary>>,
            gen_tcp:send(Sock, Concatenated),
            inet:setopts(Sock, [{active, once}]),
            user(Sock, User, Login, Room, Album);
        {line, Data} ->
            io:format("~p received ~p~n", [Self,Data]),
            gen_tcp:send(Sock, Data),
            inet:setopts(Sock, [{active, once}]),
            user(Sock, User, Login, Room, Album);
        {list, Albums} ->
            io:format("user-list ~p~n", [Albums]),
            gen_tcp:send(Sock, Albums),
            inet:setopts(Sock, [{active, once}]),
            user(Sock, User, Login, Room, Album);
        {register, ok, _User} ->
            gen_tcp:send(Sock, <<"registered\n">>),
            inet:setopts(Sock, [{active, once}]),
            Room ! {enter, Self},
            user(Sock, _User, Login, Room, Album);
        {register, nok} ->
            gen_tcp:send(Sock, <<"user already exists\n">>),
            inet:setopts(Sock, [{active, once}]),
            user(Sock, User, Login, Room, Album);
        {login, ok, _User} ->
            gen_tcp:send(Sock, <<"logged in\n">>),
            inet:setopts(Sock, [{active, once}]),
            Room ! {enter, Self},
            user(Sock, _User, Login, Room, Album);
        {login, nok} ->
            gen_tcp:send(Sock, <<"wrong user or password\n">>),
            inet:setopts(Sock, [{active, once}]),
            user(Sock, User, Login, Room, Album);
        {tcp, _, Data} ->
            io:format("~p received command ~p~n", [Self,Data]),

            case User of
                {} ->
                    case Data of
                        <<"/help\n", _/binary>> ->
                            gen_tcp:send(Sock, <<"Commands: /register, /login, /quit\n">>),
                            inet:setopts(Sock, [{active, once}]),
                            user(Sock, User, Login, Room, Album);
                        <<"/quit\n", _/binary>> ->
                            ok;
                        <<"/register ", UserPassword/binary>> ->
                            [_User, Password] = binary:split(UserPassword, <<" ">>),
                            _Password = trim_newline(Password),
                            Login ! {register, Self, {_User, _Password}},
                            inet:setopts(Sock, [{active, once}]),
                            user(Sock, User, Login, Room, Album);
                        <<"/login ", UserPassword/binary>> ->
                            [_User, Password] = binary:split(UserPassword, <<" ">>),
                            _Password = trim_newline(Password),
                            Login ! {login, Self, {_User, _Password}},
                            inet:setopts(Sock, [{active, once}]),
                            user(Sock, User, Login, Room, Album);
                        _ ->
                            gen_tcp:send(Sock, <<"unknown command\n">>),
                            inet:setopts(Sock, [{active, once}]),
                            user(Sock, User, Login, Room, Album)
                    end;
                _ ->
                    
                    case Album of
                        {} ->
                            case Data of
                                <<"/help\n", _/binary>> ->
                                    gen_tcp:send(Sock, <<"Commands: /quit, /send, /list, /createAlbum, /enterAlbum, /logout\n">>),
                                    inet:setopts(Sock, [{active, once}]),
                                    user(Sock, User, Login, Room, Album);
                                <<"/quit\n", _/binary>> ->
                                    Room ! {leave, Self};
                                <<"/send ", Rest/binary>> ->
                                    io:format("~p sending ~p~n", [Self, Rest]),
                                    Room ! {line, {Self, User, Rest}},
                                    user(Sock, User, Login, Room, Album);
                                <<"/list\n", _/binary>> ->
                                    Room ! {list, Self, User},
                                    user(Sock, User, Login, Room, Album);
                                <<"/createAlbum ", Nome/binary>> ->
                                    _Nome = trim_newline(Nome),
                                    Room ! {createAlbum, Self, User, _Nome},
                                    inet:setopts(Sock, [{active, once}]),
                                    user(Sock, User, Login, Room, Album);
                                <<"/enterAlbum ", Nome/binary>> ->
                                    _Nome = trim_newline(Nome),
                                    Room ! {enterAlbum, Self, User, _Nome},
                                    inet:setopts(Sock, [{active, once}]),
                                    user(Sock, User, Login, Room, Album);
                                <<"/logout\n", _/binary>> ->
                                    Room ! {leave, Self},
                                    user(Sock, {}, Login, Room, Album);
                                _ ->
                                    gen_tcp:send(Sock, <<"unknown command\n">>),
                                    inet:setopts(Sock, [{active, once}]),
                                    user(Sock, User, Login, Room, Album)
                            end;
                        _ ->
                            case Data of
                                <<"/help\n", _/binary>> ->
                                    gen_tcp:send(Sock, <<"Commands: /quit, /send, /addUser, /logout\n">>),
                                    inet:setopts(Sock, [{active, once}]),
                                    user(Sock, User, Login, Room, Album);
                                <<"/quit\n", _/binary>> ->
                                    Album ! {leave, Self},
                                    Room ! {enter, Self},
                                    user(Sock, User, Login, Room, {});
                                <<"/send ", Rest/binary>> ->
                                    io:format("~p sending ~p~n", [Self, Rest]),
                                    Album ! {line, {Self, User, Rest}},
                                    user(Sock, User, Login, Room, Album);
                                <<"/logout\n", _/binary>> ->
                                    Album ! {leave, Self},
                                    user(Sock, {}, Login, Room, {});
                                <<"/addUser ", _User/binary>> ->
                                    io:format("~p adding user ~p~n", [User, _User]),
                                    __User = trim_newline(_User),
                                    Album ! {addUser, Self, __User},
                                    inet:setopts(Sock, [{active, once}]),
                                    user(Sock, User, Login, Room, Album);
                                _ ->
                                    gen_tcp:send(Sock, <<"unknown command\n">>),
                                    inet:setopts(Sock, [{active, once}]),
                                    user(Sock, User, Login, Room, Album)
                            end
                    end
            end;
        {tcp_closed, _} ->
            Room ! {leave, self()};
        {tcp_error, _, _} ->
            Room ! {leave, self()}
    end.

