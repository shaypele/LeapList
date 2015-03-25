package utils;

class PaddedPrimitive<T> {
  volatile T value;
  public PaddedPrimitive(T value) {
    this.value = value;
  }
}

class PaddedPrimitiveNonVolatile<T> {
  T value;
  public PaddedPrimitiveNonVolatile(T value) {
    this.value = value;
  }
}

