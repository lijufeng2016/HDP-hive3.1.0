set hive.vectorized.execution.enabled=false;
set hive.mapred.mode=nonstrict;
set hive.stats.column.autogather=false;

drop table if exists test_txt;
drop table if exists test_orc_n1;
create table test_txt(
	c1 varchar(64),
	c2 int,
	c3 int,
	c4 char(4),
	c5 decimal(16, 10),
	c6 boolean,
	c7 float,
	c8 int,
	c9 varchar(64),
	c10 string,
	c11 int,
	c12 boolean,
	c13 int,
	c14 string,
	c15 boolean,
	c16 int,
	c17 varchar(64),
	c18 int,
	c19 int,
	c20 string,
	c21 string,
	c22 decimal(16, 10),
	c23 int,
	c24 char(4),
	c25 varchar(64),
	c26 boolean,
	c27 string,
	c28 varchar(64),
	c29 boolean,
	c30 decimal(16, 10),
	c31 varchar(64),
	c32 varchar(64),
	c33 varchar(64),
	c34 decimal(16, 10),
	c35 char(4),
	c36 string,
	c37 int,
	c38 float,
	c39 float,
	c40 varchar(64),
	c41 int,
	c42 int,
	c43 varchar(64),
	c44 char(4),
	c45 int,
	c46 int,
	c47 int,
	c48 boolean,
	c49 int,
	c50 float,
	c51 char(4),
	c52 float,
	c53 int,
	c54 int,
	c55 decimal(16, 10),
	c56 float,
	c57 string,
	c58 varchar(64),
	c59 int,
	c60 boolean,
	c61 varchar(64),
	c62 decimal(16, 10),
	c63 int,
	c64 float,
	c65 char(4),
	c66 boolean,
	c67 decimal(16, 10),
	c68 int,
	c69 decimal(16, 10),
	c70 char(4),
	c71 decimal(16, 10),
	c72 string,
	c73 string,
	c74 int,
	c75 int,
	c76 float,
	c77 char(4),
	c78 varchar(64),
	c79 decimal(16, 10),
	c80 int,
	c81 int,
	c82 decimal(16, 10),
	c83 int,
	c84 varchar(64),
	c85 decimal(16, 10),
	c86 int,
	c87 float,
	c88 decimal(16, 10),
	c89 char(4),
	c90 decimal(16, 10),
	c91 int,
	c92 string,
	c93 int,
	c94 int,
	c95 int,
	c96 int,
	c97 int,
	c98 decimal(16, 10),
	c99 float,
	c100 boolean,
	c101 varchar(64),
	c102 int,
	c103 float,
	c104 varchar(64),
	c105 decimal(16, 10),
	c106 decimal(16, 10),
	c107 char(4),
	c108 char(4),
	c109 decimal(16, 10),
	c110 float,
	c111 float,
	c112 decimal(16, 10),
	c113 string,
	c114 varchar(64),
	c115 varchar(64),
	c116 float,
	c117 int,
	c118 int,
	c119 int,
	c120 int,
	c121 char(4),
	c122 int,
	c123 float,
	c124 varchar(64),
	c125 string,
	c126 string,
	c127 int,
	c128 varchar(64),
	c129 varchar(64),
	c130 float,
	c131 char(4),
	c132 varchar(64),
	c133 varchar(64),
	c134 decimal(16, 10),
	c135 varchar(64),
	c136 char(4),
	c137 int,
	c138 decimal(16, 10),
	c139 char(4),
	c140 decimal(16, 10),
	c141 string,
	c142 char(4),
	c143 boolean,
	c144 varchar(64),
	c145 varchar(64),
	c146 int,
	c147 decimal(16, 10),
	c148 varchar(64),
	c149 float,
	c150 int,
	c151 string,
	c152 string,
	c153 varchar(64),
	c154 int,
	c155 varchar(64),
	c156 int,
	c157 float,
	c158 decimal(16, 10),
	c159 varchar(64),
	c160 int,
	c161 int,
	c162 decimal(16, 10),
	c163 varchar(64),
	c164 float,
	c165 char(4),
	c166 float,
	c167 char(4),
	c168 float,
	c169 varchar(64),
	c170 char(4),
	c171 int,
	c172 int,
	c173 char(4),
	c174 int,
	c175 char(4),
	c176 int,
	c177 int,
	c178 boolean,
	c179 char(4),
	c180 string,
	c181 int,
	c182 string,
	c183 int,
	c184 int,
	c185 float,
	c186 varchar(64),
	c187 int,
	c188 int,
	c189 int,
	c190 decimal(16, 10),
	c191 char(4),
	c192 char(4),
	c193 varchar(64),
	c194 float,
	c195 boolean,
	c196 varchar(64),
	c197 varchar(64),
	c198 float,
	c199 char(4),
	c200 decimal(16, 10),
	c201 char(4),
	c202 int,
	c203 int,
	c204 string,
	c205 varchar(64),
	c206 char(4),
	c207 int,
	c208 int,
	c209 char(4),
	c210 varchar(64),
	c211 float,
	c212 int,
	c213 int,
	c214 char(4),
	c215 int,
	c216 float,
	c217 decimal(16, 10),
	c218 int,
	c219 varchar(64),
	c220 int,
	c221 varchar(64),
	c222 varchar(64),
	c223 int,
	c224 varchar(64),
	c225 int,
	c226 float,
	c227 int,
	c228 char(4),
	c229 char(4),
	c230 char(4),
	c231 int,
	c232 float,
	c233 int,
	c234 int,
	c235 varchar(64),
	c236 float,
	c237 varchar(64),
	c238 string,
	c239 varchar(64),
	c240 int,
	c241 int,
	c242 int,
	c243 char(4),
	c244 float,
	c245 int,
	c246 char(4),
	c247 float,
	c248 varchar(64),
	c249 varchar(64),
	c250 char(4),
	c251 int,
	c252 float,
	c253 int,
	c254 decimal(16, 10),
	c255 char(4),
	c256 int,
	c257 int,
	c258 string,
	c259 float,
	c260 varchar(64),
	c261 varchar(64),
	c262 int,
	c263 string,
	c264 varchar(64),
	c265 int,
	c266 boolean,
	c267 varchar(64),
	c268 varchar(64),
	c269 varchar(64),
	c270 int,
	c271 varchar(64),
	c272 int,
	c273 char(4),
	c274 int,
	c275 int,
	c276 char(4),
	c277 char(4),
	c278 decimal(16, 10),
	c279 varchar(64),
	c280 float,
	c281 string,
	c282 string,
	c283 int,
	c284 char(4),
	c285 float,
	c286 boolean,
	c287 char(4),
	c288 varchar(64),
	c289 int,
	c290 int,
	c291 int,
	c292 varchar(64),
	c293 varchar(64),
	c294 float,
	c295 int,
	c296 decimal(16, 10),
	c297 float,
	c298 float,
	c299 char(4),
	c300 decimal(16, 10),
	c301 char(4),
	c302 varchar(64),
	c303 varchar(64),
	c304 char(4),
	c305 int,
	c306 boolean,
	c307 float,
	c308 float,
	c309 int,
	c310 char(4),
	c311 char(4),
	c312 char(4),
	c313 char(4),
	c314 int,
	c315 int,
	c316 char(4),
	c317 char(4),
	c318 decimal(16, 10),
	c319 float,
	c320 float,
	c321 varchar(64),
	c322 string,
	c323 varchar(64),
	c324 varchar(64),
	c325 float,
	c326 varchar(64),
	c327 varchar(64),
	c328 float,
	c329 char(4),
	c330 int,
	c331 decimal(16, 10),
	c332 int,
	c333 varchar(64),
	c334 decimal(16, 10),
	c335 int,
	c336 varchar(64),
	c337 int,
	c338 varchar(64),
	c339 int,
	c340 int,
	c341 int,
	c342 char(4),
	c343 varchar(64),
	c344 decimal(16, 10),
	c345 int,
	c346 char(4),
	c347 int,
	c348 varchar(64),
	c349 int,
	c350 string,
	c351 char(4),
	c352 int,
	c353 int,
	c354 int,
	c355 int,
	c356 int,
	c357 float,
	c358 varchar(64),
	c359 boolean,
	c360 varchar(64),
	c361 int,
	c362 float,
	c363 char(4),
	c364 varchar(64),
	c365 int,
	c366 int,
	c367 decimal(16, 10),
	c368 decimal(16, 10),
	c369 int,
	c370 decimal(16, 10),
	c371 char(4),
	c372 char(4),
	c373 char(4),
	c374 string,
	c375 float,
	c376 boolean,
	c377 float,
	c378 varchar(64),
	c379 char(4),
	c380 varchar(64),
	c381 varchar(64),
	c382 char(4),
	c383 int,
	c384 char(4),
	c385 boolean,
	c386 float,
	c387 varchar(64),
	c388 string,
	c389 decimal(16, 10),
	c390 decimal(16, 10),
	c391 float,
	c392 boolean,
	c393 float,
	c394 int,
	c395 varchar(64),
	c396 decimal(16, 10),
	c397 decimal(16, 10),
	c398 varchar(64),
	c399 boolean,
	c400 float,
	c401 int,
	c402 int,
	c403 char(4),
	c404 float,
	c405 string,
	c406 varchar(64),
	c407 decimal(16, 10),
	c408 int,
	c409 varchar(64),
	c410 int,
	c411 int,
	c412 char(4),
	c413 float,
	c414 int,
	c415 char(4),
	c416 int,
	c417 int,
	c418 float,
	c419 int,
	c420 int,
	c421 int,
	c422 float,
	c423 varchar(64),
	c424 char(4),
	c425 varchar(64),
	c426 float,
	c427 int,
	c428 varchar(64),
	c429 float,
	c430 int,
	c431 char(4),
	c432 decimal(16, 10),
	c433 varchar(64),
	c434 int,
	c435 string,
	c436 int,
	c437 int,
	c438 float,
	c439 char(4),
	c440 int,
	c441 int,
	c442 varchar(64),
	c443 int,
	c444 int,
	c445 float,
	c446 int,
	c447 boolean,
	c448 int,
	c449 int,
	c450 char(4),
	c451 int,
	c452 boolean,
	c453 varchar(64),
	c454 char(4),
	c455 varchar(64),
	c456 int,
	c457 int,
	c458 varchar(64),
	c459 float,
	c460 boolean,
	c461 varchar(64),
	c462 char(4),
	c463 varchar(64),
	c464 int,
	c465 string,
	c466 varchar(64),
	c467 boolean,
	c468 string,
	c469 float,
	c470 float,
	c471 int,
	c472 varchar(64),
	c473 char(4),
	c474 decimal(16, 10),
	c475 char(4),
	c476 float,
	c477 char(4),
	c478 float,
	c479 int,
	c480 int,
	c481 int,
	c482 decimal(16, 10),
	c483 decimal(16, 10),
	c484 int,
	c485 int,
	c486 float,
	c487 char(4),
	c488 char(4),
	c489 varchar(64),
	c490 int,
	c491 varchar(64),
	c492 int,
	c493 decimal(16, 10),
	c494 int,
	c495 int,
	c496 int,
	c497 varchar(64),
	c498 decimal(16, 10),
	c499 float,
	c500 char(4),
	c501 decimal(16, 10),
	c502 char(4),
	c503 decimal(16, 10),
	c504 string,
	c505 boolean,
	c506 boolean,
	c507 char(4),
	c508 char(4),
	c509 int,
	c510 float,
	c511 decimal(16, 10),
	c512 float,
	c513 char(4),
	c514 varchar(64),
	c515 string,
	c516 char(4),
	c517 float,
	c518 int,
	c519 int,
	c520 decimal(16, 10),
	c521 int,
	c522 decimal(16, 10),
	c523 int,
	c524 boolean,
	c525 int,
	c526 int,
	c527 char(4),
	c528 varchar(64),
	c529 float,
	c530 decimal(16, 10),
	c531 char(4),
	c532 string,
	c533 char(4),
	c534 boolean,
	c535 boolean,
	c536 int,
	c537 float,
	c538 int,
	c539 int,
	c540 int,
	c541 decimal(16, 10),
	c542 int,
	c543 float,
	c544 float,
	c545 float,
	c546 boolean,
	c547 float,
	c548 float,
	c549 int,
	c550 int,
	c551 int,
	c552 int,
	c553 int,
	c554 float,
	c555 char(4),
	c556 char(4),
	c557 int,
	c558 int,
	c559 decimal(16, 10),
	c560 varchar(64),
	c561 int,
	c562 decimal(16, 10),
	c563 decimal(16, 10),
	c564 string,
	c565 decimal(16, 10),
	c566 int,
	c567 boolean,
	c568 char(4),
	c569 float,
	c570 string,
	c571 decimal(16, 10),
	c572 int,
	c573 float,
	c574 boolean,
	c575 float,
	c576 float,
	c577 varchar(64),
	c578 varchar(64),
	c579 int,
	c580 char(4),
	c581 varchar(64),
	c582 int,
	c583 decimal(16, 10),
	c584 char(4),
	c585 decimal(16, 10),
	c586 int,
	c587 varchar(64),
	c588 float,
	c589 int,
	c590 int,
	c591 char(4),
	c592 float,
	c593 varchar(64),
	c594 float,
	c595 varchar(64),
	c596 int,
	c597 int,
	c598 char(4),
	c599 varchar(64),
	c600 int,
	c601 int,
	c602 int,
	c603 string,
	c604 int,
	c605 float,
	c606 char(4),
	c607 boolean,
	c608 int,
	c609 int,
	c610 varchar(64),
	c611 float,
	c612 varchar(64),
	c613 int,
	c614 boolean,
	c615 int,
	c616 int,
	c617 float,
	c618 char(4),
	c619 decimal(16, 10),
	c620 varchar(64),
	c621 varchar(64),
	c622 int,
	c623 char(4),
	c624 int,
	c625 varchar(64),
	c626 int,
	c627 int,
	c628 char(4),
	c629 varchar(64),
	c630 char(4),
	c631 decimal(16, 10),
	c632 varchar(64),
	c633 varchar(64),
	c634 varchar(64),
	c635 float,
	c636 int,
	c637 varchar(64),
	c638 string,
	c639 int,
	c640 int,
	c641 int,
	c642 float,
	c643 char(4),
	c644 int,
	c645 varchar(64),
	c646 float,
	c647 varchar(64),
	c648 char(4),
	c649 decimal(16, 10),
	c650 char(4),
	c651 varchar(64),
	c652 varchar(64),
	c653 int,
	c654 decimal(16, 10),
	c655 float,
	c656 varchar(64),
	c657 int,
	c658 char(4),
	c659 float,
	c660 int,
	c661 int,
	c662 string,
	c663 int,
	c664 int,
	c665 char(4),
	c666 char(4),
	c667 float,
	c668 int,
	c669 char(4),
	c670 int,
	c671 int,
	c672 varchar(64),
	c673 int,
	c674 string,
	c675 varchar(64),
	c676 float,
	c677 varchar(64),
	c678 char(4),
	c679 varchar(64),
	c680 decimal(16, 10),
	c681 int,
	c682 int,
	c683 string,
	c684 varchar(64),
	c685 int,
	c686 string,
	c687 char(4),
	c688 varchar(64),
	c689 varchar(64),
	c690 string,
	c691 float,
	c692 int,
	c693 float,
	c694 int,
	c695 float,
	c696 char(4),
	c697 float,
	c698 int,
	c699 float,
	c700 varchar(64),
	c701 varchar(64),
	c702 int,
	c703 float,
	c704 char(4),
	c705 float,
	c706 float,
	c707 decimal(16, 10),
	c708 varchar(64),
	c709 char(4),
	c710 boolean,
	c711 varchar(64),
	c712 int,
	c713 boolean,
	c714 float,
	c715 int,
	c716 int,
	c717 int,
	c718 int,
	c719 char(4),
	c720 int,
	c721 varchar(64),
	c722 int,
	c723 int,
	c724 int,
	c725 varchar(64),
	c726 varchar(64),
	c727 int,
	c728 decimal(16, 10),
	c729 char(4),
	c730 int,
	c731 string,
	c732 decimal(16, 10),
	c733 boolean,
	c734 varchar(64),
	c735 char(4),
	c736 int,
	c737 int,
	c738 string,
	c739 decimal(16, 10),
	c740 varchar(64),
	c741 int,
	c742 varchar(64),
	c743 string,
	c744 char(4),
	c745 int,
	c746 char(4),
	c747 varchar(64),
	c748 int,
	c749 int,
	c750 int,
	c751 char(4),
	c752 boolean,
	c753 varchar(64),
	c754 float,
	c755 char(4),
	c756 char(4),
	c757 int,
	c758 int,
	c759 float,
	c760 varchar(64),
	c761 float,
	c762 boolean,
	c763 int,
	c764 int,
	c765 int,
	c766 int,
	c767 decimal(16, 10),
	c768 int,
	c769 int,
	c770 char(4),
	c771 varchar(64),
	c772 char(4),
	c773 decimal(16, 10),
	c774 varchar(64),
	c775 char(4),
	c776 float,
	c777 string,
	c778 int,
	c779 float,
	c780 char(4),
	c781 char(4),
	c782 char(4),
	c783 char(4),
	c784 int,
	c785 decimal(16, 10),
	c786 decimal(16, 10),
	c787 varchar(64),
	c788 int,
	c789 char(4),
	c790 char(4),
	c791 varchar(64),
	c792 int,
	c793 decimal(16, 10),
	c794 int,
	c795 int,
	c796 int,
	c797 int,
	c798 int,
	c799 int,
	c800 int,
	c801 varchar(64),
	c802 decimal(16, 10),
	c803 float,
	c804 int,
	c805 int,
	c806 int,
	c807 int,
	c808 char(4),
	c809 int,
	c810 float,
	c811 boolean,
	c812 decimal(16, 10),
	c813 int,
	c814 decimal(16, 10),
	c815 varchar(64),
	c816 varchar(64),
	c817 float,
	c818 float,
	c819 int,
	c820 varchar(64),
	c821 boolean,
	c822 int,
	c823 varchar(64),
	c824 varchar(64),
	c825 decimal(16, 10),
	c826 char(4),
	c827 varchar(64),
	c828 char(4),
	c829 float,
	c830 decimal(16, 10),
	c831 varchar(64),
	c832 varchar(64),
	c833 int,
	c834 float,
	c835 varchar(64),
	c836 int,
	c837 string,
	c838 char(4),
	c839 int,
	c840 int,
	c841 char(4),
	c842 int,
	c843 float,
	c844 int,
	c845 int,
	c846 boolean,
	c847 float,
	c848 decimal(16, 10),
	c849 int,
	c850 int,
	c851 int,
	c852 int,
	c853 int,
	c854 char(4),
	c855 varchar(64),
	c856 int,
	c857 int,
	c858 decimal(16, 10),
	c859 decimal(16, 10),
	c860 boolean,
	c861 int,
	c862 int,
	c863 float,
	c864 string,
	c865 int,
	c866 decimal(16, 10),
	c867 varchar(64),
	c868 char(4),
	c869 float,
	c870 string,
	c871 varchar(64),
	c872 char(4),
	c873 int,
	c874 int,
	c875 int,
	c876 decimal(16, 10),
	c877 float,
	c878 float,
	c879 decimal(16, 10),
	c880 decimal(16, 10),
	c881 int,
	c882 int,
	c883 boolean,
	c884 float,
	c885 char(4),
	c886 char(4),
	c887 decimal(16, 10),
	c888 float,
	c889 decimal(16, 10),
	c890 int,
	c891 varchar(64),
	c892 float,
	c893 int,
	c894 int,
	c895 float,
	c896 char(4),
	c897 boolean,
	c898 int,
	c899 int,
	c900 float,
	c901 int,
	c902 int,
	c903 int,
	c904 char(4),
	c905 decimal(16, 10),
	c906 int,
	c907 char(4),
	c908 int,
	c909 decimal(16, 10),
	c910 char(4),
	c911 decimal(16, 10),
	c912 varchar(64),
	c913 char(4),
	c914 boolean,
	c915 string,
	c916 varchar(64),
	c917 char(4),
	c918 int,
	c919 float,
	c920 char(4),
	c921 int,
	c922 char(4),
	c923 decimal(16, 10),
	c924 int,
	c925 float,
	c926 boolean,
	c927 int,
	c928 decimal(16, 10),
	c929 int,
	c930 decimal(16, 10),
	c931 int,
	c932 int,
	c933 varchar(64),
	c934 varchar(64),
	c935 int,
	c936 varchar(64),
	c937 string,
	c938 char(4),
	c939 decimal(16, 10),
	c940 int,
	c941 char(4),
	c942 int,
	c943 int,
	c944 varchar(64),
	c945 float,
	c946 int,
	c947 decimal(16, 10),
	c948 decimal(16, 10),
	c949 char(4),
	c950 int,
	c951 int,
	c952 varchar(64),
	c953 float,
	c954 boolean,
	c955 float,
	c956 varchar(64),
	c957 char(4),
	c958 char(4),
	c959 decimal(16, 10),
	c960 int,
	c961 int,
	c962 int,
	c963 float,
	c964 varchar(64),
	c965 char(4),
	c966 float,
	c967 char(4),
	c968 decimal(16, 10),
	c969 int,
	c970 varchar(64),
	c971 int,
	c972 varchar(64),
	c973 char(4),
	c974 int,
	c975 varchar(64),
	c976 decimal(16, 10),
	c977 boolean,
	c978 int,
	c979 int,
	c980 char(4),
	c981 decimal(16, 10),
	c982 int,
	c983 decimal(16, 10),
	c984 varchar(64),
	c985 int,
	c986 char(4),
	c987 decimal(16, 10),
	c988 string,
	c989 int,
	c990 float,
	c991 int,
	c992 int,
	c993 char(4),
	c994 decimal(16, 10),
	c995 boolean,
	c996 int,
	c997 int,
	c998 varchar(64),
	c999 char(4),
	c1000 int,
	c1001 boolean,
	c1002 varchar(64),
	c1003 varchar(64),
	c1004 float,
	c1005 float,
	c1006 int,
	c1007 decimal(16, 10),
	c1008 float,
	c1009 varchar(64),
	c1010 boolean,
	c1011 char(4),
	c1012 float,
	c1013 char(4),
	c1014 varchar(64),
	c1015 char(4),
	c1016 decimal(16, 10),
	c1017 char(4),
	c1018 float,
	c1019 varchar(64),
	c1020 int,
	c1021 float,
	c1022 decimal(16, 10),
	c1023 varchar(64),
	c1024 char(4),
	c1025 float,
	c1026 float,
	c1027 int,
	c1028 int,
	c1029 char(4),
	c1030 decimal(16, 10),
	c1031 char(4),
	c1032 varchar(64),
	c1033 varchar(64),
	c1034 char(4),
	c1035 varchar(64),
	c1036 decimal(16, 10),
	c1037 int,
	c1038 float,
	c1039 float,
	c1040 varchar(64),
	c1041 varchar(64),
	c1042 float,
	c1043 int,
	c1044 boolean,
	c1045 string,
	c1046 string,
	c1047 char(4),
	c1048 float,
	c1049 varchar(64),
	c1050 varchar(64),
	c1051 float,
	c1052 int,
	c1053 char(4),
	c1054 float,
	c1055 decimal(16, 10),
	c1056 string,
	c1057 char(4),
	c1058 varchar(64),
	c1059 char(4),
	c1060 int,
	c1061 float,
	c1062 string,
	c1063 varchar(64),
	c1064 int,
	c1065 varchar(64),
	c1066 char(4),
	c1067 char(4),
	c1068 string,
	c1069 string,
	c1070 varchar(64),
	c1071 char(4),
	c1072 int,
	c1073 float,
	c1074 varchar(64),
	c1075 float,
	c1076 char(4),
	c1077 decimal(16, 10),
	c1078 char(4),
	c1079 int,
	c1080 char(4),
	c1081 int,
	c1082 float,
	c1083 int,
	c1084 float,
	c1085 int,
	c1086 boolean,
	c1087 float,
	c1088 float,
	c1089 decimal(16, 10),
	c1090 float,
	c1091 decimal(16, 10),
	c1092 char(4),
	c1093 int,
	c1094 float,
	c1095 decimal(16, 10),
	c1096 varchar(64),
	c1097 decimal(16, 10),
	c1098 varchar(64),
	c1099 varchar(64),
	c1100 boolean,
	c1101 decimal(16, 10),
	c1102 char(4),
	c1103 char(4),
	c1104 decimal(16, 10),
	c1105 decimal(16, 10),
	c1106 boolean,
	c1107 varchar(64),
	c1108 char(4),
	c1109 float,
	c1110 decimal(16, 10),
	c1111 decimal(16, 10),
	c1112 int,
	c1113 int,
	c1114 float,
	c1115 string,
	c1116 char(4),
	c1117 float,
	c1118 varchar(64),
	c1119 int,
	c1120 varchar(64),
	c1121 int,
	c1122 varchar(64),
	c1123 int,
	c1124 int,
	c1125 char(4),
	c1126 int,
	c1127 varchar(64),
	c1128 int,
	c1129 char(4),
	c1130 boolean,
	c1131 varchar(64),
	c1132 float,
	c1133 char(4),
	c1134 float,
	c1135 int,
	c1136 float,
	c1137 string,
	c1138 int,
	c1139 int,
	c1140 float,
	c1141 char(4),
	c1142 float,
	c1143 int,
	c1144 decimal(16, 10),
	c1145 int,
	c1146 float,
	c1147 boolean,
	c1148 int,
	c1149 boolean,
	c1150 float,
	c1151 varchar(64),
	c1152 decimal(16, 10),
	c1153 string,
	c1154 int,
	c1155 string,
	c1156 string,
	c1157 varchar(64),
	c1158 varchar(64),
	c1159 decimal(16, 10),
	c1160 char(4),
	c1161 char(4),
	c1162 int,
	c1163 int,
	c1164 float,
	c1165 int,
	c1166 int,
	c1167 varchar(64),
	c1168 int,
	c1169 varchar(64),
	c1170 float,
	c1171 char(4),
	c1172 varchar(64),
	c1173 int,
	c1174 int,
	c1175 char(4),
	c1176 int,
	c1177 char(4),
	c1178 float,
	c1179 int,
	c1180 int,
	c1181 varchar(64),
	c1182 boolean,
	c1183 char(4),
	c1184 char(4),
	c1185 int,
	c1186 boolean,
	c1187 float,
	c1188 float,
	c1189 decimal(16, 10),
	c1190 varchar(64),
	c1191 float,
	c1192 int,
	c1193 varchar(64),
	c1194 float,
	c1195 decimal(16, 10),
	c1196 char(4),
	c1197 int,
	c1198 int,
	c1199 string,
	c1200 int,
	c1201 char(4),
	c1202 int,
	c1203 varchar(64),
	c1204 int,
	c1205 varchar(64),
	c1206 char(4),
	c1207 int,
	c1208 float,
	c1209 int,
	c1210 decimal(16, 10),
	c1211 decimal(16, 10),
	c1212 decimal(16, 10),
	c1213 int,
	c1214 int,
	c1215 varchar(64),
	c1216 boolean,
	c1217 decimal(16, 10),
	c1218 float,
	c1219 int,
	c1220 int,
	c1221 varchar(64),
	c1222 char(4),
	c1223 varchar(64),
	c1224 string,
	c1225 varchar(64),
	c1226 varchar(64),
	c1227 int,
	c1228 float,
	c1229 int,
	c1230 char(4),
	c1231 varchar(64),
	c1232 int,
	c1233 int,
	c1234 int,
	c1235 string,
	c1236 char(4),
	c1237 float,
	c1238 int,
	c1239 int,
	c1240 decimal(16, 10),
	c1241 char(4),
	c1242 string,
	c1243 int,
	c1244 int,
	c1245 int,
	c1246 float,
	c1247 char(4),
	c1248 varchar(64),
	c1249 int,
	c1250 int,
	c1251 int,
	c1252 varchar(64),
	c1253 float,
	c1254 decimal(16, 10),
	c1255 boolean,
	c1256 int,
	c1257 int,
	c1258 int,
	c1259 int,
	c1260 float,
	c1261 float,
	c1262 decimal(16, 10),
	c1263 varchar(64),
	c1264 int,
	c1265 varchar(64),
	c1266 float,
	c1267 char(4),
	c1268 float,
	c1269 float,
	c1270 decimal(16, 10),
	c1271 int,
	c1272 varchar(64),
	c1273 string,
	c1274 int,
	c1275 char(4),
	c1276 char(4),
	c1277 boolean,
	c1278 float,
	c1279 int,
	c1280 int,
	c1281 char(4),
	c1282 decimal(16, 10),
	c1283 boolean,
	c1284 decimal(16, 10),
	c1285 boolean,
	c1286 varchar(64),
	c1287 char(4),
	c1288 char(4),
	c1289 varchar(64),
	c1290 int,
	c1291 char(4),
	c1292 varchar(64),
	c1293 float,
	c1294 int,
	c1295 int,
	c1296 float,
	c1297 int,
	c1298 char(4),
	c1299 varchar(64),
	c1300 char(4),
	c1301 char(4),
	c1302 char(4),
	c1303 decimal(16, 10),
	c1304 varchar(64),
	c1305 varchar(64),
	c1306 char(4),
	c1307 varchar(64),
	c1308 int,
	c1309 decimal(16, 10),
	c1310 varchar(64),
	c1311 boolean,
	c1312 int,
	c1313 float,
	c1314 float,
	c1315 char(4),
	c1316 varchar(64),
	c1317 float,
	c1318 varchar(64),
	c1319 int,
	c1320 varchar(64),
	c1321 int,
	c1322 char(4),
	c1323 float,
	c1324 float,
	c1325 varchar(64),
	c1326 decimal(16, 10),
	c1327 varchar(64),
	c1328 varchar(64),
	c1329 char(4),
	c1330 char(4),
	c1331 float,
	c1332 int,
	c1333 char(4),
	c1334 int,
	c1335 varchar(64),
	c1336 int,
	c1337 char(4),
	c1338 decimal(16, 10),
	c1339 varchar(64),
	c1340 int,
	c1341 varchar(64),
	c1342 decimal(16, 10),
	c1343 char(4),
	c1344 int,
	c1345 float,
	c1346 float,
	c1347 int,
	c1348 char(4),
	c1349 varchar(64),
	c1350 int,
	c1351 char(4),
	c1352 int,
	c1353 int,
	c1354 float,
	c1355 varchar(64),
	c1356 int,
	c1357 float,
	c1358 varchar(64),
	c1359 varchar(64),
	c1360 decimal(16, 10),
	c1361 varchar(64),
	c1362 int,
	c1363 int,
	c1364 int,
	c1365 char(4),
	c1366 int,
	c1367 varchar(64),
	c1368 int,
	c1369 char(4),
	c1370 string,
	c1371 varchar(64),
	c1372 varchar(64),
	c1373 int,
	c1374 char(4),
	c1375 decimal(16, 10),
	c1376 float,
	c1377 char(4),
	c1378 float,
	c1379 float,
	c1380 int,
	c1381 decimal(16, 10),
	c1382 int,
	c1383 int,
	c1384 int,
	c1385 boolean,
	c1386 char(4),
	c1387 varchar(64),
	c1388 int,
	c1389 varchar(64),
	c1390 float,
	c1391 decimal(16, 10),
	c1392 string,
	c1393 varchar(64),
	c1394 int,
	c1395 decimal(16, 10),
	c1396 int,
	c1397 int,
	c1398 varchar(64),
	c1399 float,
	c1400 string,
	c1401 boolean,
	c1402 boolean,
	c1403 int,
	c1404 float,
	c1405 varchar(64),
	c1406 decimal(16, 10),
	c1407 decimal(16, 10),
	c1408 varchar(64),
	c1409 char(4),
	c1410 int,
	c1411 int,
	c1412 int,
	c1413 varchar(64),
	c1414 int,
	c1415 decimal(16, 10),
	c1416 int,
	c1417 varchar(64),
	c1418 varchar(64),
	c1419 int,
	c1420 char(4),
	c1421 string,
	c1422 decimal(16, 10),
	c1423 int,
	c1424 int,
	c1425 char(4),
	c1426 int,
	c1427 int,
	c1428 int,
	c1429 float,
	c1430 char(4),
	c1431 int,
	c1432 decimal(16, 10),
	c1433 int,
	c1434 float,
	c1435 int,
	c1436 decimal(16, 10),
	c1437 boolean,
	c1438 float,
	c1439 char(4),
	c1440 varchar(64),
	c1441 float,
	c1442 int,
	c1443 char(4),
	c1444 int,
	c1445 int,
	c1446 int,
	c1447 boolean,
	c1448 int,
	c1449 int,
	c1450 float,
	c1451 varchar(64),
	c1452 float,
	c1453 string,
	c1454 int,
	c1455 char(4),
	c1456 char(4),
	c1457 float,
	c1458 float,
	c1459 float,
	c1460 decimal(16, 10),
	c1461 float,
	c1462 int,
	c1463 boolean,
	c1464 decimal(16, 10),
	c1465 int,
	c1466 string,
	c1467 varchar(64),
	c1468 char(4),
	c1469 varchar(64),
	c1470 int,
	c1471 varchar(64),
	c1472 varchar(64),
	c1473 char(4),
	c1474 decimal(16, 10),
	c1475 int,
	c1476 int,
	c1477 int,
	c1478 float,
	c1479 string,
	c1480 varchar(64),
	c1481 float,
	c1482 int,
	c1483 char(4),
	c1484 int,
	c1485 float,
	c1486 float,
	c1487 float,
	c1488 varchar(64),
	c1489 varchar(64),
	c1490 int,
	c1491 char(4),
	c1492 varchar(64),
	c1493 char(4),
	c1494 int,
	c1495 char(4),
	c1496 varchar(64),
	c1497 char(4),
	c1498 decimal(16, 10),
	c1499 int,
	c1500 char(4),
	c1501 int,
	c1502 int,
	c1503 int,
	c1504 varchar(64),
	c1505 char(4),
	c1506 int,
	c1507 int,
	c1508 varchar(64),
	c1509 boolean,
	c1510 char(4),
	c1511 int,
	c1512 string,
	c1513 char(4),
	c1514 int,
	c1515 int,
	c1516 string,
	c1517 float,
	c1518 float,
	c1519 int,
	c1520 boolean,
	c1521 int,
	c1522 varchar(64),
	c1523 varchar(64),
	c1524 varchar(64),
	c1525 float,
	c1526 varchar(64),
	c1527 char(4),
	c1528 varchar(64),
	c1529 varchar(64),
	c1530 int,
	c1531 char(4),
	c1532 int,
	c1533 int,
	c1534 varchar(64),
	c1535 int,
	c1536 string,
	c1537 int,
	c1538 string,
	c1539 char(4),
	c1540 float,
	c1541 string,
	c1542 int,
	c1543 varchar(64),
	c1544 int,
	c1545 string,
	c1546 decimal(16, 10),
	c1547 varchar(64),
	c1548 char(4),
	c1549 decimal(16, 10),
	c1550 char(4),
	c1551 char(4),
	c1552 float,
	c1553 varchar(64),
	c1554 varchar(64),
	c1555 float,
	c1556 char(4),
	c1557 int,
	c1558 boolean,
	c1559 decimal(16, 10),
	c1560 string,
	c1561 string,
	c1562 varchar(64),
	c1563 float,
	c1564 float,
	c1565 int,
	c1566 boolean,
	c1567 int,
	c1568 char(4),
	c1569 varchar(64),
	c1570 boolean,
	c1571 int,
	c1572 int,
	c1573 int,
	c1574 int,
	c1575 float,
	c1576 float,
	c1577 decimal(16, 10),
	c1578 decimal(16, 10),
	c1579 float,
	c1580 float,
	c1581 int,
	c1582 decimal(16, 10),
	c1583 decimal(16, 10),
	c1584 int,
	c1585 float,
	c1586 decimal(16, 10),
	c1587 float,
	c1588 int,
	c1589 float,
	c1590 char(4),
	c1591 char(4),
	c1592 float,
	c1593 string,
	c1594 decimal(16, 10),
	c1595 int,
	c1596 varchar(64),
	c1597 boolean,
	c1598 float,
	c1599 int,
	c1600 float,
	c1601 float,
	c1602 char(4),
	c1603 boolean,
	c1604 float,
	c1605 char(4),
	c1606 int,
	c1607 string,
	c1608 int,
	c1609 int,
	c1610 char(4),
	c1611 string,
	c1612 char(4),
	c1613 float,
	c1614 int,
	c1615 int,
	c1616 varchar(64),
	c1617 varchar(64),
	c1618 varchar(64),
	c1619 int,
	c1620 float,
	c1621 varchar(64),
	c1622 float,
	c1623 int,
	c1624 float,
	c1625 varchar(64),
	c1626 int,
	c1627 int,
	c1628 varchar(64),
	c1629 int,
	c1630 decimal(16, 10),
	c1631 float,
	c1632 char(4),
	c1633 char(4),
	c1634 boolean,
	c1635 decimal(16, 10),
	c1636 decimal(16, 10),
	c1637 int,
	c1638 varchar(64),
	c1639 varchar(64),
	c1640 int,
	c1641 float,
	c1642 float,
	c1643 float,
	c1644 varchar(64),
	c1645 char(4),
	c1646 varchar(64),
	c1647 varchar(64),
	c1648 int,
	c1649 int,
	c1650 int,
	c1651 string,
	c1652 int,
	c1653 char(4),
	c1654 decimal(16, 10),
	c1655 float,
	c1656 string,
	c1657 char(4),
	c1658 float,
	c1659 int,
	c1660 decimal(16, 10),
	c1661 char(4),
	c1662 float,
	c1663 float,
	c1664 char(4),
	c1665 decimal(16, 10),
	c1666 char(4),
	c1667 string,
	c1668 char(4),
	c1669 varchar(64),
	c1670 decimal(16, 10),
	c1671 decimal(16, 10),
	c1672 int,
	c1673 int,
	c1674 int,
	c1675 int,
	c1676 char(4),
	c1677 char(4),
	c1678 varchar(64),
	c1679 varchar(64),
	c1680 decimal(16, 10),
	c1681 decimal(16, 10),
	c1682 varchar(64),
	c1683 int,
	c1684 int,
	c1685 varchar(64),
	c1686 string,
	c1687 varchar(64),
	c1688 int,
	c1689 int,
	c1690 varchar(64),
	c1691 string,
	c1692 decimal(16, 10),
	c1693 char(4),
	c1694 string,
	c1695 string,
	c1696 varchar(64),
	c1697 varchar(64),
	c1698 char(4),
	c1699 float,
	c1700 char(4),
	c1701 varchar(64),
	c1702 varchar(64),
	c1703 int,
	c1704 varchar(64),
	c1705 float,
	c1706 decimal(16, 10),
	c1707 char(4),
	c1708 boolean,
	c1709 string,
	c1710 varchar(64),
	c1711 float,
	c1712 int,
	c1713 varchar(64),
	c1714 varchar(64),
	c1715 int,
	c1716 varchar(64),
	c1717 int,
	c1718 int,
	c1719 varchar(64),
	c1720 int,
	c1721 varchar(64),
	c1722 string,
	c1723 char(4),
	c1724 char(4),
	c1725 float,
	c1726 boolean,
	c1727 int,
	c1728 float,
	c1729 varchar(64),
	c1730 varchar(64),
	c1731 int,
	c1732 varchar(64),
	c1733 int,
	c1734 int,
	c1735 varchar(64),
	c1736 char(4),
	c1737 char(4),
	c1738 boolean,
	c1739 varchar(64),
	c1740 string,
	c1741 char(4),
	c1742 varchar(64),
	c1743 char(4),
	c1744 decimal(16, 10),
	c1745 int,
	c1746 float,
	c1747 float,
	c1748 int,
	c1749 char(4),
	c1750 boolean,
	c1751 varchar(64),
	c1752 int,
	c1753 int,
	c1754 int,
	c1755 float,
	c1756 varchar(64),
	c1757 varchar(64),
	c1758 int,
	c1759 int,
	c1760 decimal(16, 10),
	c1761 int,
	c1762 int,
	c1763 int,
	c1764 char(4),
	c1765 decimal(16, 10),
	c1766 char(4),
	c1767 char(4),
	c1768 decimal(16, 10),
	c1769 string,
	c1770 varchar(64),
	c1771 varchar(64),
	c1772 float,
	c1773 int,
	c1774 int,
	c1775 varchar(64),
	c1776 decimal(16, 10),
	c1777 float,
	c1778 varchar(64),
	c1779 varchar(64),
	c1780 float,
	c1781 varchar(64),
	c1782 float,
	c1783 varchar(64),
	c1784 int,
	c1785 char(4),
	c1786 boolean,
	c1787 decimal(16, 10),
	c1788 int,
	c1789 int,
	c1790 int,
	c1791 float,
	c1792 varchar(64),
	c1793 varchar(64),
	c1794 decimal(16, 10),
	c1795 int,
	c1796 int,
	c1797 varchar(64),
	c1798 char(4),
	c1799 int,
	c1800 decimal(16, 10),
	c1801 int,
	c1802 int,
	c1803 int,
	c1804 float,
	c1805 float,
	c1806 char(4),
	c1807 varchar(64),
	c1808 int,
	c1809 string,
	c1810 char(4),
	c1811 float,
	c1812 int,
	c1813 varchar(64),
	c1814 varchar(64),
	c1815 int,
	c1816 char(4),
	c1817 boolean,
	c1818 char(4),
	c1819 string,
	c1820 int,
	c1821 float,
	c1822 string,
	c1823 varchar(64),
	c1824 varchar(64),
	c1825 char(4),
	c1826 int,
	c1827 varchar(64),
	c1828 varchar(64),
	c1829 char(4),
	c1830 decimal(16, 10),
	c1831 int,
	c1832 decimal(16, 10),
	c1833 int,
	c1834 decimal(16, 10),
	c1835 boolean,
	c1836 int,
	c1837 float,
	c1838 decimal(16, 10),
	c1839 decimal(16, 10),
	c1840 char(4),
	c1841 boolean,
	c1842 varchar(64),
	c1843 varchar(64),
	c1844 int,
	c1845 varchar(64),
	c1846 varchar(64),
	c1847 varchar(64),
	c1848 char(4),
	c1849 string,
	c1850 char(4),
	c1851 float,
	c1852 float,
	c1853 int,
	c1854 varchar(64),
	c1855 float,
	c1856 varchar(64),
	c1857 char(4),
	c1858 float,
	c1859 int,
	c1860 float,
	c1861 int,
	c1862 string,
	c1863 char(4),
	c1864 decimal(16, 10),
	c1865 string,
	c1866 varchar(64),
	c1867 char(4),
	c1868 float,
	c1869 varchar(64),
	c1870 varchar(64),
	c1871 char(4),
	c1872 int,
	c1873 float,
	c1874 int,
	c1875 float,
	c1876 int,
	c1877 float,
	c1878 decimal(16, 10),
	c1879 boolean,
	c1880 varchar(64),
	c1881 varchar(64),
	c1882 float,
	c1883 int,
	c1884 string,
	c1885 varchar(64),
	c1886 char(4),
	c1887 char(4),
	c1888 int,
	c1889 char(4),
	c1890 char(4),
	c1891 decimal(16, 10),
	c1892 int,
	c1893 char(4),
	c1894 float,
	c1895 string,
	c1896 boolean,
	c1897 int,
	c1898 varchar(64),
	c1899 char(4),
	c1900 float,
	c1901 float,
	c1902 float,
	c1903 float,
	c1904 float,
	c1905 char(4),
	c1906 char(4),
	c1907 char(4),
	c1908 decimal(16, 10),
	c1909 float,
	c1910 int,
	c1911 char(4),
	c1912 boolean,
	c1913 int,
	c1914 int,
	c1915 varchar(64),
	c1916 int,
	c1917 varchar(64),
	c1918 int,
	c1919 string,
	c1920 decimal(16, 10),
	c1921 float,
	c1922 float,
	c1923 int,
	c1924 int,
	c1925 float,
	c1926 decimal(16, 10),
	c1927 string,
	c1928 float,
	c1929 float,
	c1930 int,
	c1931 varchar(64),
	c1932 float,
	c1933 varchar(64),
	c1934 int,
	c1935 int,
	c1936 boolean,
	c1937 float,
	c1938 float,
	c1939 char(4),
	c1940 varchar(64),
	c1941 int,
	c1942 char(4),
	c1943 varchar(64),
	c1944 boolean,
	c1945 int,
	c1946 int,
	c1947 float,
	c1948 int,
	c1949 varchar(64),
	c1950 decimal(16, 10),
	c1951 string,
	c1952 varchar(64),
	c1953 int,
	c1954 varchar(64),
	c1955 float,
	c1956 decimal(16, 10),
	c1957 int,
	c1958 int,
	c1959 varchar(64),
	c1960 char(4),
	c1961 int,
	c1962 char(4),
	c1963 varchar(64),
	c1964 int,
	c1965 float,
	c1966 char(4),
	c1967 float,
	c1968 boolean,
	c1969 decimal(16, 10),
	c1970 float,
	c1971 varchar(64),
	c1972 float,
	c1973 int,
	c1974 float,
	c1975 int,
	c1976 char(4),
	c1977 varchar(64),
	c1978 string,
	c1979 int,
	c1980 varchar(64),
	c1981 char(4),
	c1982 float,
	c1983 int,
	c1984 float,
	c1985 boolean,
	c1986 int,
	c1987 boolean,
	c1988 int,
	c1989 varchar(64),
	c1990 int,
	c1991 varchar(64),
	c1992 char(4),
	c1993 boolean,
	c1994 varchar(64),
	c1995 boolean,
	c1996 int,
	c1997 string,
	c1998 varchar(64),
	c1999 int,
	c2000 boolean
) row format delimited fields terminated by ',';
load data local inpath '../../data/files/2000_cols_data.csv' overwrite into table test_txt;
create table test_orc_n1 like test_txt;
alter table test_orc_n1 set fileformat orc;
insert into table test_orc_n1 select * from test_txt;

select c1, c2, c1999 from test_txt order by c1;
select c1, c2, c1999 from test_orc_n1 order by c1;

