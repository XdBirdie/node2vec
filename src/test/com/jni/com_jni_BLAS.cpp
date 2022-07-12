#include <jni.h>
#include <graphblas/graphblas.hpp>
#include <vector>
#include "com_jni_BLAS.h"


JNIEXPORT jdoubleArray JNICALL Java_com_jni_BLAS_vxm_1minPlus
  (JNIEnv * env, jobject obj, jintArray src, jintArray dst, jdoubleArray val, jintArray vid,jdoubleArray vec, jint n, jint m)
{
    jint *srcArray = env->GetIntArrayElements(src,NULL);
    jint *dstArray = env->GetIntArrayElements(dst,NULL);
    jint *vidArray = env->GetIntArrayElements(vid,NULL);
    jdouble *valArray = env->GetDoubleArrayElements(val,NULL);
    jdouble *vecArray = env->GetDoubleArrayElements(vec,NULL);
    int edgNum = env->GetArrayLength(src);
    int pointNum = env->GetArrayLength(vid);
    using T = grb::IndexType;
    using GBMatrix = grb::Matrix<T, grb::DirectedMatrixTag>;
    GBMatrix G_tn(n, m);
    G_tn.build(srcArray,dstArray,valArray,edgNum);
    grb::Vector<int> vect(n);
    grb::Vector<int> ans(n);
    for(int i = 0;i<pointNum;i++)
      vect.setElement(vidArray[i],vecArray[i]);
    grb::vxm(ans,grb::NoMask(),grb::NoAccumulate(),grb::MinPlusSemiring<grb::IndexType>(),vect,G_tn,grb::REPLACE);
    jdoubleArray ret=env->NewDoubleArray(n);
    jdouble *retArr=env->GetDoubleArrayElements(ret,NULL);
    for(int i=0;i<n;i++)
      if(ans.hasElement(i))
        retArr[i] = ans.extractElement(i);
      else retArr[i] = -1;
    env->ReleaseIntArrayElements(src,srcArray,0);
    env->ReleaseIntArrayElements(dst,dstArray,0);
    env->ReleaseIntArrayElements(vid,vidArray,0);
    env->ReleaseDoubleArrayElements(vec,vecArray,0);
    env->ReleaseDoubleArrayElements(ret,retArr,0);
    env->ReleaseDoubleArrayElements(val,valArray,0);
    return ret;

}

JNIEXPORT jdoubleArray JNICALL Java_com_jni_BLAS_vxm_1plusTimes
  (JNIEnv * env, jobject obj, jintArray src, jintArray dst, jdoubleArray val, jintArray vid,jdoubleArray vec, jint n, jint m)
{
    jint *srcArray = env->GetIntArrayElements(src,NULL);
    jint *dstArray = env->GetIntArrayElements(dst,NULL);
    jint *vidArray = env->GetIntArrayElements(vid,NULL);
    jdouble *valArray = env->GetDoubleArrayElements(val,NULL);
    jdouble *vecArray = env->GetDoubleArrayElements(vec,NULL);
    int edgNum = env->GetArrayLength(src);
    int pointNum = env->GetArrayLength(vid);
    using T = grb::IndexType;
    using GBMatrix = grb::Matrix<T, grb::DirectedMatrixTag>;
    GBMatrix G_tn(n, m);
    G_tn.build(srcArray,dstArray,valArray,edgNum);
    grb::Vector<int> vect(n);
    grb::Vector<int> ans(n);
    for(int i = 0;i<pointNum;i++)
      vect.setElement(vidArray[i],vecArray[i]);
    grb::vxm(ans,grb::NoMask(),grb::NoAccumulate(),grb::ArithmeticSemiring<grb::IndexType>(),vect,G_tn,grb::REPLACE);
    jdoubleArray ret=env->NewDoubleArray(n);
    jdouble *retArr=env->GetDoubleArrayElements(ret,NULL);
    for(int i=0;i<n;i++)
      if(ans.hasElement(i))
        retArr[i] = ans.extractElement(i);
      else retArr[i] = -1;
    env->ReleaseIntArrayElements(src,srcArray,0);
    env->ReleaseIntArrayElements(dst,dstArray,0);
    env->ReleaseIntArrayElements(vid,vidArray,0);
    env->ReleaseDoubleArrayElements(vec,vecArray,0);
    env->ReleaseDoubleArrayElements(ret,retArr,0);
    env->ReleaseDoubleArrayElements(val,valArray,0);
    return ret;

}
