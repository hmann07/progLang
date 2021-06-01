

%my append to take two lists a return a combined version


myappend([],List,List).
myappend([H1|T1],List,[H1|T2]) :- myappend(T1,List,T2).

myfib(0,0):-!.
myfib(1,1):-!.
myfib(X,Results) :- A is X-1, B is X-2, myfib(A,A1), myfib(B,B1), Results is B1 + A1.


app([],List,List).
app([H1|T1], List, [H1|T2]) :- app(T1,List,T2).


suffix(Suff,Rest, Result) :- app(Rest,Suff,Result).


mysum([],0).
mysum([H1|T1],X) :- mysum(T1,Y), X is Y + H1.


sqrandsum([],0).
sqrandsum([H1|T1],X):- sqrandsum(T1,Y), X is Y + (H1*H1).

mycount([],0).
mycount([_|T1],Result):- mycount(T1,NewBind), Result is NewBind + 1.

myfac(0,1).
myfac(N,Result) :- N2 is N-1, myfac(N2,B), Result is B * N.

myreverse([],A,A):-!.
myreverse([H1|T1],List,Result):- myreverse(T1,[H1|List],Result).

rev(X,Z):-myreverse(X,[],Z).


mymin([],Acc,Acc).
mymin([H|T],Y,X):- Y is if mymin(T,Y)
