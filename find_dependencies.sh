if [ "$1" == "" ]; then
  echo "usage: <command> <root>"
  exit 1
fi

ROOT=$1
rm -rf lib include bin etc
mkdir lib
mkdir include
mkdir bin
mkdir etc
cp $ROOT/CPackConfig.cmake $ROOT/LICENSE.txt $ROOT/README.md .

ROOTS=$(find $ROOT -name "*libHSQueryArrow-ffi-c*.so" | grep "install")
ROOT0="$ROOT/.stack-work/install/x86_64-linux/lts-7.9/8.0.1/bin/QueryArrow $ROOT/.stack-work/install/x86_64-linux/lts-7.9/8.0.1/bin/QueryArrowServer"
ROOTS2="$ROOTS $ROOT0"
LIBS1=$(ldd -v $ROOTS2 | grep "=>" | grep libHS | awk '/=>/{print $(NF-1)}' | sort -u | grep "install")
LIBS3=$(ldd -v $ROOTS2 | grep "=>" | grep libHS | awk '/=>/{print $(NF-1)}' | sort -u | grep "snapshots")
for i in $ROOTS; do
    i2=$(basename $i)
    i3=${i2##*-}
    i4=${i2%-*}
    i5=${i4%-*}
    i6=$i5-$i3
    cp $i lib
    ln -s $i2 lib/$i6
done
cp $ROOT0 bin
cp -r $ROOT/QueryArrow-gen/etc/* etc
for i in $LIBS1; do
    i2=$(basename $i)
    i3=${i2##*-}
    i4=${i2%-*}
    i5=${i4%-*}
    i6=$i5-$i3
    cp $i lib
    ln -s $i2 lib/$i6
done
for i in $LIBS3; do
    i2=$(basename $i)
    i3=${i2##*-}
    i4=${i2%-*}
    i5=${i4%-*}
    i6=$i5-$i3
    cp $i lib
    ln -s $i2 lib/$i6
done

INCLUDES=$(find $ROOT -name "*_stub.h")
for i in $INCLUDES; do
    cp $i include
done
