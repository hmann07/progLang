inlist(X,[X|_]).
inlist(X,[H|T]) :- inlist(X,T).


%inlist(z,[x,y,z]).

%1 call inlist (z, [x, y, z])
%2 call inlist (z, [y,z])
%3 call inlist(z, [z])
%exit 3
%exit 2
%exit 1



myfac(1,1).
myfac(X,Z):- Y is X-1, myfac(Y,B), Z is X*B.


trfacaux(1,Acc,Acc) :-!.
trfacaux(X,Acc,Res) :- N is X-1, V is Acc * X, trfacaux(N,V,Res).

trfac(X,Z):- trfacaux(X,1,Z).
